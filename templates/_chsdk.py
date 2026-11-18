"""Run local payloads alongside a best-effort remote ComputeHorde hello-world job."""

import argparse
import asyncio
import codecs
import io
import locale
import logging
import os
import subprocess
import sys
import threading
import time

from compute_horde_sdk.v1.local import (
    LocalDockerJobSpec,
    LocalBindVolume,
    LocalCleanupError,
    LocalCleanupTimeoutError,
    LocalDockerClient,
    LocalDockerJob,
    LocalJobResult,
    LocalJobStatus,
    LocalJobTimeoutError,
)

from . import _common

logger = logging.getLogger(__name__)

DEFAULT_CHSDK_VALIDATOR_HOTKEY = "5Hjbf5s2Cb4xuYGLKUGVEkwQrMpSMYvnatJxVenMnjodtEKi"


def _remote_keys():
    """Use the validator's wallet options without changing its configuration code."""
    from bittensor_wallet import Wallet

    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False, exit_on_error=False)
    parser.add_argument("--wallet.name", dest="name", default="default")
    parser.add_argument("--wallet.hotkey", dest="hotkey", default="default")
    parser.add_argument("--wallet.path", dest="path", default="~/.bittensor/wallets")
    options, _ = parser.parse_known_args()
    signing_key = Wallet(
        name=os.getenv("WALLET_NAME") or options.name,
        hotkey=os.getenv("WALLET_HOTKEY") or options.hotkey,
        path=options.path,
    ).hotkey
    target = os.getenv("CHSDK_VALIDATOR_HOTKEY") or DEFAULT_CHSDK_VALIDATOR_HOTKEY
    return signing_key, target


async def _remote_hello_world(name, deadline):
    """Bound submission and polling; remote failures must never replace local results."""
    client = None
    job = None

    async def submit_and_wait():
        nonlocal client, job
        from compute_horde_sdk.v1 import ComputeHordeClient, ComputeHordeJobSpec, ExecutorClass

        signing_key, target = await asyncio.to_thread(_remote_keys)
        client = ComputeHordeClient(
            hotkey=signing_key,
            compute_horde_validator_hotkey=target,
            facilitator_url="https://facilitator.computehorde.io/",
        )
        logger.info("CHSDK remote hello-world for %s: submitting to validator %s", name, target)
        job = await client.create_job(ComputeHordeJobSpec(
            executor_class=ExecutorClass.always_on__cpu__4c__8gb,
            job_namespace="SN107.0",
            docker_image="hello-world:latest",
            artifacts_dir="",
            download_time_limit_sec=30,
            execution_time_limit_sec=10,
            upload_time_limit_sec=10,
        ))
        logger.info("CHSDK remote hello-world for %s: job %s submitted", name, job.uuid)

        async def show_status(remote_job, entry):
            logger.info("CHSDK remote hello-world for %s: job %s status %s",
                        name, remote_job.uuid, entry.status.value)

        await job.wait(status_callback=show_status)
        if job.result is not None:
            logger.info("CHSDK remote hello-world for %s: job %s stdout=%r stderr=%r",
                        name, job.uuid, job.result.stdout, job.result.stderr)
        if job.status.is_successful():
            logger.info("CHSDK remote hello-world for %s: job %s succeeded", name, job.uuid)
        else:
            logger.warning("CHSDK remote hello-world for %s: job %s failed: %s",
                           name, job.uuid, job.error or job.status)

    remaining = _common._remaining(deadline)
    budget = 300 if remaining is None else min(300, remaining)
    logger.info("CHSDK remote hello-world for %s: starting alongside local job (budget %.1fs)",
                name, budget)
    try:
        await asyncio.wait_for(submit_and_wait(), timeout=budget)
    except asyncio.TimeoutError:
        logger.warning("CHSDK remote hello-world for %s: timed out after %.1fs (job %s); "
                       "local result is unaffected", name, budget, job.uuid if job else "UUID unavailable")
    except Exception:
        logger.warning("CHSDK remote hello-world for %s failed; local result is unaffected",
                       name, exc_info=True)
    finally:
        if client is not None:
            try:
                # SDK 0.0.20 has no public close method.
                await asyncio.wait_for(client._client.aclose(), timeout=5)
            except Exception:
                logger.warning("CHSDK remote hello-world for %s: could not close client",
                               name, exc_info=True)


def parse_command(cmd: list[str]) -> LocalDockerJobSpec:
    """Parse only the Docker run options used by the validator, before IMAGE."""
    if cmd[:2] != ["docker", "run"]:
        raise ValueError("CHSDK runner expects docker run")
    values, volumes, env = {}, [], {}
    remove = False
    index = 2
    fields = {"--name": "container_name", "--cpus": "cpus", "--memory": "memory",
              "-u": "user", "--platform": "platform"}
    while index < len(cmd) and cmd[index].startswith("-"):
        token = cmd[index]
        index += 1
        if token == "--rm":
            remove = True
            continue
        option, equals, value = token.partition("=")
        if option not in {*fields, "-v", "-e"}:
            raise ValueError(f"Unsupported Docker option in CHSDK mode: {token}")
        if not equals:
            if index == len(cmd):
                raise ValueError(f"Missing value for {option}")
            value = cmd[index]
            index += 1
        if option == "-v":
            parts = value.split(":")
            if len(parts) not in (2, 3) or (len(parts) == 3 and parts[2] not in ("ro", "rw")):
                raise ValueError(f"Unsupported bind mount: {value}")
            if not parts[0].startswith("/"):
                raise ValueError(f"Bind source must be an absolute host path: {value}")
            volumes.append(LocalBindVolume(
                source=parts[0], destination=parts[1],
                read_only=len(parts) == 3 and parts[2] == "ro",
            ))
        elif option == "-e":
            key, separator, val = value.partition("=")
            if not separator or not key:
                raise ValueError("CHSDK mode requires explicit -e KEY=VALUE")
            env[key] = val
        else:
            values[fields[option]] = float(value) if option == "--cpus" else value
    if not remove or not values.get("container_name") or index == len(cmd):
        raise ValueError("CHSDK runner requires --rm, --name and an image")
    return LocalDockerJobSpec(
        docker_image=cmd[index], args=cmd[index + 1:], volumes=volumes, env=env, **values,
    )


class _Output:
    """Keep subprocess capture semantics; open stdout files receive raw bytes."""

    def __init__(self, kwargs):
        supported = {"capture_output", "stdout", "stderr", "text", "universal_newlines",
                     "encoding", "errors", "timeout", "check"}
        unknown = kwargs.keys() - supported
        if unknown:
            raise TypeError(f"Unsupported CHSDK subprocess kwargs: {sorted(unknown)}")
        stdout, stderr = kwargs.get("stdout"), kwargs.get("stderr")
        if kwargs.get("capture_output"):
            if stdout is not None or stderr is not None:
                raise ValueError("stdout/stderr cannot be used with capture_output")
            stdout = stderr = subprocess.PIPE
        if stderr not in (None, subprocess.PIPE):
            raise ValueError("CHSDK stderr supports PIPE or inheritance only")
        if ("text" in kwargs and "universal_newlines" in kwargs
                and bool(kwargs["text"]) != bool(kwargs["universal_newlines"])):
            raise ValueError("text and universal_newlines disagree")
        self.text = any(kwargs.get(k) for k in ("text", "universal_newlines", "encoding", "errors"))
        self.encoding = kwargs.get("encoding") or locale.getpreferredencoding(False)
        self.errors = kwargs.get("errors") or "strict"
        codecs.lookup(self.encoding)
        codecs.lookup_error(self.errors)
        self.capture_stderr = stderr == subprocess.PIPE
        self.sink = None
        if stdout != subprocess.PIPE:
            target = sys.stdout if stdout is None else stdout
            if not isinstance(target, (io.TextIOWrapper, io.BufferedIOBase, io.RawIOBase)):
                raise ValueError("CHSDK stdout supports PIPE, inheritance or an open file")
            target.flush()
            self.sink = target.buffer if isinstance(target, io.TextIOWrapper) else target

    def decode(self, value):
        if value is None or not self.text:
            return value
        return value.decode(self.encoding, self.errors).replace("\r\n", "\n").replace("\r", "\n")

    def completed(self, cmd, result: LocalJobResult, check) -> subprocess.CompletedProcess:
        if not self.capture_stderr and result.stderr:
            sys.stderr.buffer.write(result.stderr)
            sys.stderr.buffer.flush()
        process = subprocess.CompletedProcess(
            cmd, result.returncode, self.decode(result.stdout),
            self.decode(result.stderr) if self.capture_stderr else None,
        )
        if check:
            process.check_returncode()
        return process

    def timeout(self, cmd, timeout, partial: LocalJobResult | None) -> subprocess.TimeoutExpired:
        if partial is not None and not self.capture_stderr and partial.stderr:
            sys.stderr.buffer.write(partial.stderr)
            sys.stderr.buffer.flush()
        return subprocess.TimeoutExpired(
            cmd, timeout, output=partial.stdout if partial else None,
            stderr=partial.stderr if partial and self.capture_stderr else None,
        )


class _Runner:
    def __init__(self):
        self.client = LocalDockerClient(cleanup_timeout=30)
        self.jobs: dict[str, LocalDockerJob] = {}
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, name="minos-chsdk", daemon=True)
        self.thread.start()

    def call(self, coroutine):
        return asyncio.run_coroutine_threadsafe(coroutine, self.loop).result()

    async def run(self, cmd, spec, output, round_id, deadline, timeout, check):
        name = spec.container_name
        with _common._live_lock:
            if round_id is not None and _common._open_round != round_id:
                raise _common.RegistryClosed(f"round {round_id} has ended; not starting {name}")
            if name in self.jobs:
                raise ValueError(f"CHSDK container name already registered: {name}")
            # prepare_job does no I/O. Both identity and cancellation exist before
            # the CHSDK can issue create/start requests on this same event loop.
            job = self.client.prepare_job(spec, stdout=output.sink)
            self.jobs[name] = job
            _common._live_containers.add(name)
            job.start()
        remote = asyncio.create_task(_remote_hello_world(name, deadline))
        cleanup_confirmed = True
        try:
            try:
                result = await job.wait(timeout=_common._remaining(deadline))
            except LocalCleanupError as exc:
                cleanup_confirmed = False
                logger.warning("CHSDK cleanup pending for %s; retaining handle: %s", name, exc)
                if (isinstance(exc, LocalCleanupTimeoutError)
                        or (exc.result is not None and exc.result.status == LocalJobStatus.TIMED_OUT)):
                    raise output.timeout(cmd, timeout, exc.result) from exc
                raise
            except LocalJobTimeoutError as exc:
                raise output.timeout(cmd, timeout, exc.result) from exc
            return output.completed(cmd, result, check)
        finally:
            with _common._live_lock:
                _common._live_containers.discard(name)
                if cleanup_confirmed and self.jobs.get(name) is job:
                    del self.jobs[name]
            # Join even if the local job failed. The stub handles its own errors,
            # preserving the local result/exception and keeping its output separate.
            await remote

    async def stop(self, name: str, job: LocalDockerJob, deadline: float) -> bool:
        try:
            await job.stop(timeout=_common._remaining(deadline))
        except Exception as exc:  # cleanup must not mask the original failure
            logger.warning("Could not reap CHSDK container %s: %s", name, exc)
            return False
        with _common._live_lock:
            if self.jobs.get(name) is job:
                del self.jobs[name]
        return True

    async def reap(self, name, deadline):
        with _common._live_lock:
            _common._live_containers.discard(name)
            job = self.jobs.get(name)
            if job is not None:
                job.request_stop()
        if job is not None and await self.stop(name, job, deadline):
            logger.warning("Reaped orphaned CHSDK container %s", name)

    async def reap_all(self, deadline):
        with _common._live_lock:
            _common._open_round = None
            _common._live_containers.clear()
            # Include unconfirmed removals from earlier cleanup attempts.
            snapshot = list(self.jobs.items())
            for _, job in snapshot:
                job.request_stop()
        removed = sum(await asyncio.gather(
            *(self.stop(name, job, deadline) for name, job in snapshot)
        ))
        if removed:
            logger.warning("Reaped %d CHSDK container job(s)", removed)
        return removed


_runner: _Runner | None = None
_runner_lock = threading.Lock()


def _get_runner() -> _Runner:
    global _runner
    with _runner_lock:
        if _runner is None:
            _runner = _Runner()
        return _runner


def run_container(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    timeout = kwargs.get("timeout")
    deadline = None if timeout is None else time.monotonic() + timeout
    output = _Output(kwargs)
    spec = parse_command(cmd)
    runner = _get_runner()
    try:
        return runner.call(runner.run(
            cmd, spec, output, _common._job_round.get(), deadline, timeout, kwargs.get("check", False),
        ))
    except (KeyboardInterrupt, SystemExit):
        # Interruption of the synchronous waiter does not cancel the CHSDK task.
        reap_container(spec.container_name)
        raise


def reap_container(name: str) -> None:
    if not name:
        return
    deadline = time.monotonic() + 30
    runner = _get_runner()
    runner.call(runner.reap(name, deadline))


def reap_live_containers(budget_seconds: float = 20) -> int:
    deadline = time.monotonic() + budget_seconds
    runner = _get_runner()
    return runner.call(runner.reap_all(deadline))
