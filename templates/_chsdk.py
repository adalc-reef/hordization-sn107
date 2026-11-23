"""Local Compute Horde runner with a round-scoped GATK comparison."""

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

from . import _common, _chsdk_compare
from ._chsdk_compare import ComparisonRound

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
        self.rounds: dict[int, ComparisonRound] = {}
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, name="minos-chsdk", daemon=True)
        self.thread.start()

    def call(self, coroutine):
        return asyncio.run_coroutine_threadsafe(coroutine, self.loop).result()

    async def run(self, cmd, spec, output, round_id, deadline, timeout, check):
        is_gatk = (spec.docker_image.startswith("broadinstitute/gatk:")
                   and spec.args[:2] == ("gatk", "--java-options") and spec.args[3:4] == ("HaplotypeCaller",))
        if not is_gatk:
            return await self.run_local(cmd, spec, output, round_id, deadline, timeout, check)

        started = time.monotonic()
        with _common._live_lock:
            ctx = self.rounds.get(round_id) if round_id == _common._open_round else None
        deadlines = [d for d in (deadline, ctx.deadline if ctx else None) if d is not None]
        remote_deadline = min(deadlines, default=None)
        miner = "unknown"
        if "-O" in spec.args[:-1]:
            filename = spec.args[spec.args.index("-O") + 1].rsplit("/", 1)[-1]
            miner = filename.removeprefix("miner_").removesuffix(".vcf.gz")
        round_label = ctx.round_id if ctx else round_id
        logger.info("CHSDK remote GATK starting comparison round=%s miner=%s attempt=%s",
                    round_label, miner, spec.container_name)
        remote = asyncio.create_task(_chsdk_compare.run_remote(ctx, spec, remote_deadline, _remote_keys))
        if ctx:
            ctx.users.add(remote)
            remote.add_done_callback(ctx.users.discard)
        local_status = "failed"
        try:
            local = await self.run_local(cmd, spec, output, round_id, deadline, timeout, check)
            local_status = "ok" if local.returncode == 0 else f"exit {local.returncode}"
            return local
        except Exception as exc:
            local_status = type(exc).__name__
            raise
        finally:
            local_elapsed = time.monotonic() - started
            remote_status = "round closed"
            remote_elapsed = 0.0
            outcome = "unavailable"
            reason = ""
            try:
                result, remote_status, remote_elapsed = await remote
                if remote_status != "ok":
                    reason = remote_status
                elif local_status != "ok":
                    result[0].cleanup()
                    reason = "local caller failed"
                else:
                    outcome = await asyncio.wait_for(asyncio.to_thread(_chsdk_compare.compare, *result,
                                                                       remote_deadline),
                                                     _common._remaining(remote_deadline))
                    reason = ("VCF columns or records differ" if outcome == "mismatch" else
                              "indexed VCF columns and records agree")
            except (asyncio.CancelledError, Exception) as exc:
                reason = str(exc) if isinstance(exc, _chsdk_compare.Unavailable) else type(exc).__name__
            logger.info("GATK comparison round=%s miner=%s attempt=%s local=%s local_seconds=%.3f "
                        "remote=%s remote_seconds=%.3f result=%s reason=%s",
                        round_label, miner, spec.container_name, local_status, local_elapsed,
                        remote_status, remote_elapsed, outcome, reason)

    async def run_local(self, cmd, spec, output, round_id, deadline, timeout, check):
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

    async def reap_all(self, deadline, generation=None):
        with _common._live_lock:
            if generation is not None and _common._open_round not in (None, generation):
                # A watchdog queued on the validator's executor can arrive late.
                return 0
            ctx = self.rounds.pop(_common._open_round, None)
            _common._open_round = None
            _common._live_containers.clear()
            # Include unconfirmed removals from earlier cleanup attempts.
            snapshot = list(self.jobs.items())
            for _, job in snapshot:
                job.request_stop()
        cleanup = ctx.close() if ctx else None
        removed = sum(await asyncio.gather(
            *(self.stop(name, job, deadline) for name, job in snapshot)
        ))
        if cleanup:
            await asyncio.wait([cleanup], timeout=_common._remaining(deadline))
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


def bind_round(ctx, reference, region, remaining):
    runner = _get_runner()
    generation = _common._job_round.get()
    ctx.bind(generation, reference, region, remaining)
    with _common._live_lock:
        if generation == _common._open_round:
            runner.rounds[generation] = ctx


def finish_round(ctx):
    runner = _get_runner()

    async def finish():
        with _common._live_lock:
            runner.rounds.pop(ctx.generation, None)
            if _common._open_round == ctx.generation:
                _common._open_round = None
        await asyncio.wait([ctx.close()], timeout=_common.REAP_BUDGET_SECONDS)

    runner.call(finish())


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
    return runner.call(runner.reap_all(deadline, _common._job_round.get()))
