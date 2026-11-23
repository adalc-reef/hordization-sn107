"""Round inputs, remote job lifetime, and GATK record comparison.

After binding, mutable execution state belongs to the CHSDK runner's event loop.
"""
import asyncio
import gzip
import hashlib
import logging
import re
import tempfile
import time
from itertools import groupby
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit
from uuid import uuid4

from compute_horde_sdk.v1 import (
    ComputeHordeClient, ComputeHordeJobSpec, ExecutorClass, HTTPInputVolume, HTTPOutputVolume,
)

from utils.path_utils import safe_round_dir_name
from utils.reference_inventory import MINER_DATA_FILES
from . import _common
from ._chsdk_storage import Store, StorageConfigurationError

logger = logging.getLogger(__name__)
_background = set()
ROUND_CACHE_TTL_SECONDS = 2 * 60 * 60
REFERENCE_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60


def background(coro):
    task = asyncio.create_task(coro)
    _background.add(task)
    task.add_done_callback(_background.discard)
    return task


class Unavailable(Exception):
    """The remote comparison could not be completed."""


def resource(url):
    try:
        parsed = urlsplit(url)
        # Preserve object selectors such as S3 versionId during metadata refresh.
        signing = {"signature", "expires", "awsaccesskeyid", "x-amz-algorithm", "x-amz-credential",
                   "x-amz-date", "x-amz-expires", "x-amz-signedheaders", "x-amz-signature",
                   "x-amz-security-token"}
        query = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k.lower() not in signing]
        return parsed.scheme, parsed.netloc, parsed.path, query
    except ValueError:
        return "", "", "", []


class ComparisonRound:
    def __init__(self, round_id, base_dir):
        self.round_id = round_id
        self.base_dir = base_dir
        self.prefix = f"chsdk/{safe_round_dir_name(round_id)}/{uuid4().hex}"
        self.bam = None
        self.urls = {}
        self.bam_identity = None
        self.candidates = ()
        self.built_index = False
        self.store_task = self.index_task = self.index_key = None
        self.generation = self.close_task = None
        self.closed = False
        self.users = set()
        self.pending_cleanup = set()
        self.index_lease_end = 0.0

    def refresh_sources(self, *bam_candidates):
        self.candidates = tuple(u for u in bam_candidates if isinstance(u, str))
        self.built_index = False
        self.urls.pop("round.bam.bai", None)

    def record_source(self, path, url):
        # Never guess which backend populated a pre-existing cache. A retry
        # within this preparation can renew the SAME observed object's signature.
        if path.name == "round.bam":
            self.bam = path
            try:
                stat = path.stat()
            except OSError:
                self.urls[path.name] = None
                return
            identity = stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns
            previous = self.urls.get(path.name)
            if not url and self.bam_identity == identity and previous:
                url = next((u for u in self.candidates if resource(u) == resource(previous)), None)
            self.bam_identity = identity
        self.urls[path.name] = url

    def bind(self, generation, reference, region, remaining):
        self.generation = generation
        self.reference = reference
        self.region = region
        self.deadline = None if remaining is None else time.monotonic() + remaining

    def round_volume(self, name, url):
        path = self.bam.with_name(name).relative_to(self.base_dir)
        return HTTPInputVolume(url=url, cache_key=path.as_posix(), cache_ttl_seconds=ROUND_CACHE_TTL_SECONDS)

    def inputs(self, spec):
        args = list(spec.args)

        def host_path(flag):
            path = Path(args[args.index(flag) + 1])
            for volume in spec.volumes:
                if path.parent == Path(volume.destination):
                    return volume.source / path.name
            raise Unavailable("unknown caller mount")

        chrom = self.region.split(":")[0]
        expected = Path("datasets/reference") / chrom / f"{chrom}.fa"
        if (self.reference != self.base_dir / expected or host_path("-R") != self.reference
                or host_path("-I") != self.bam or args[args.index("-L") + 1] != self.region):
            raise Unavailable("caller inputs differ from standard round inputs")
        refs = {Path(f["local"]): f["url"] for f in MINER_DATA_FILES}
        volumes = {}
        for path in (expected, expected.with_suffix(".fa.fai"), expected.with_suffix(".dict")):
            local = self.base_dir / path
            if path not in refs or not local.is_file() or local.resolve() != local:
                raise Unavailable("reference outside installer inventory or missing sidecar")
            volumes[f"/volume/reference/{path.name}"] = HTTPInputVolume(
                url=refs[path], cache_key=path.as_posix(), cache_ttl_seconds=REFERENCE_CACHE_TTL_SECONDS,
            )

        for name in ("round.bam", "round.bam.bai"):
            if name.endswith(".bai") and self.built_index:
                continue
            url = self.urls.get(name)
            scheme, host, *_ = resource(url or "")
            if scheme not in ("https", "http") or not host:
                raise Unavailable("missing observed HTTP source for " + name)
            volumes[f"/volume/data/{name}"] = self.round_volume(name, url)

        output = host_path("-O")
        if output.parent != self.bam.parent or not re.fullmatch(r"miner_[a-zA-Z0-9_-]+\.vcf\.gz", output.name):
            raise Unavailable("unknown miner output")
        for flag, path in (("-R", f"/volume/reference/{expected.name}"), ("-I", "/volume/data/round.bam"),
                           ("-O", f"/output/{output.name}")):
            args[args.index(flag) + 1] = path
        return volumes, args, output

    async def storage(self):
        if self.store_task is None:
            self.store_task = asyncio.create_task(asyncio.to_thread(Store))
        return await asyncio.shield(self.store_task)

    async def index_url(self, store, deadline):
        if self.index_task is None:
            self.index_key = f"{self.prefix}/round.bam.bai"
            self.index_task = asyncio.create_task(asyncio.to_thread(
                store.upload_index, self.index_key, self.bam.with_suffix(".bam.bai")))
        await asyncio.shield(self.index_task)
        return store.sign("get_object", self.index_key, deadline)

    async def retire(self):
        # A cancelled to_thread waiter cannot stop a PUT in progress.
        if self.index_task is not None:
            await asyncio.gather(self.index_task, return_exceptions=True)
            store = await self.storage()
            # The facilitator can still assign an ambiguously submitted job.
            # Keep its shared input usable for the life of its signed GET.
            delay = max(0, self.index_lease_end - time.monotonic())
            await delete_keys(store, [self.index_key], delay)
        elif self.store_task is not None:
            await asyncio.gather(self.store_task, return_exceptions=True)

    def close(self):
        if not self.closed:
            self.closed = True
            users = tuple(self.users)
            for task in users:
                task.cancel()

            async def finish():
                await asyncio.gather(*users, return_exceptions=True)
                await asyncio.gather(self.retire(), *self.pending_cleanup)
            self.close_task = background(finish())
        return self.close_task


async def delete_keys(store, keys, delay=0):
    if delay:
        await asyncio.sleep(delay)
    for key in keys:
        for attempt in range(2):
            try:
                await asyncio.to_thread(store.delete, key)
                break
            except Exception as exc:
                if attempt:
                    logger.warning("CHSDK object cleanup failed key=%s: %s", key, exc, exc_info=True)


async def _execute(ctx, spec, deadline, remote_keys):
    volumes, args, output = ctx.inputs(spec)
    store = await ctx.storage()
    if ctx.built_index:
        url = await ctx.index_url(store, deadline)
        volumes["/volume/data/round.bam.bai"] = ctx.round_volume("round.bam.bai", url)
    signing_key, target = await asyncio.to_thread(remote_keys)
    with _common._live_lock:
        if ctx.closed or ctx.generation != _common._open_round:
            raise Unavailable("round closed")
    miner = output.name.removeprefix("miner_").removesuffix(".vcf.gz")
    prefix = f"{ctx.prefix}/{miner}/{uuid4().hex}"
    files = {name: f"{prefix}/{name}" for name in (output.name, output.name + ".tbi")}
    remaining = int(deadline - time.monotonic())
    if remaining < 3:
        raise TimeoutError()
    download_budget = max(1, remaining // 5)
    upload_budget = max(1, remaining // 10)
    job_spec = ComputeHordeJobSpec(
        executor_class=ExecutorClass.always_on__cpu__4c__8gb,
        job_namespace="SN107.0",
        docker_image=spec.docker_image, args=args, artifacts_dir="",
        input_volumes=volumes,
        output_volumes={f"/output/{n}": HTTPOutputVolume(http_method="PUT", url=store.sign("put_object", k, deadline))
                        for n, k in files.items()},
        download_time_limit_sec=download_budget, execution_time_limit_sec=remaining - download_budget - upload_budget,
        upload_time_limit_sec=upload_budget,
    )
    client = ComputeHordeClient(hotkey=signing_key, compute_horde_validator_hotkey=target,
                                facilitator_url="https://facilitator.computehorde.io/")
    pending = set(files.values())
    terminal = False
    try:
        logger.info("CHSDK remote GATK submitting round=%s miner=%s attempt=%s validator=%s budget_seconds=%d",
                    ctx.round_id, miner, spec.container_name, target, remaining)
        job = await client.create_job(job_spec)
        logger.info("CHSDK remote GATK submitted attempt=%s job=%s", spec.container_name, job.uuid)

        def log_status(remote_job, entry):
            logger.info("CHSDK remote GATK job=%s status=%s", remote_job.uuid, entry.status.value)

        await job.wait(status_callback=log_status)
        terminal = True
        if job.result is not None:
            logger.info("CHSDK remote GATK job=%s stdout=%r stderr=%r", job.uuid, job.result.stdout, job.result.stderr)
        if not job.status.is_successful():
            logger.warning("CHSDK remote GATK job=%s status=%s error=%s", job.uuid, job.status.value, job.error)
            raise Unavailable("remote job " + job.status.value)
        directory = tempfile.TemporaryDirectory(prefix="minos-chsdk-")
        error = None
        for name, key in files.items():
            try:
                if await asyncio.to_thread(store.retrieve, key, directory, name, deadline):
                    pending.remove(key)
                logger.info("CHSDK remote GATK job=%s retrieved=%s", job.uuid, name)
            except Exception as exc:
                logger.warning("CHSDK remote GATK job=%s retrieval failed for %s: %s", job.uuid, name, exc,
                               exc_info=True)
                error = f"retrieving {name} failed ({type(exc).__name__})"
        if error is not None:
            directory.cleanup()
            raise Unavailable(error)
        return directory, output
    finally:
        if pending:
            cleanup = background(delete_keys(store, tuple(pending)))
            ctx.pending_cleanup.add(cleanup)
            cleanup.add_done_callback(ctx.pending_cleanup.discard)
            if not terminal:
                if ctx.built_index:
                    ctx.index_lease_end = max(ctx.index_lease_end, deadline + 1)
                # create_job may have been accepted even when it returned no UUID.
                # A PUT started before expiry can finish during its upload window.
                background(delete_keys(store, tuple(pending), max(1, deadline - time.monotonic() + upload_budget + 2)))
        background(_close_client(client))


async def _close_client(client):
    try:
        await asyncio.wait_for(client._client.aclose(), 3)
    except Exception as exc:
        logger.warning("CHSDK client close failed: %s", exc, exc_info=True)


async def run_remote(ctx, spec, deadline, remote_keys):
    started = time.monotonic()
    try:
        if ctx is None or ctx.closed or deadline is None:
            raise Unavailable("round inputs unavailable")
        result = await asyncio.wait_for(_execute(ctx, spec, deadline, remote_keys),
                                        max(0, deadline - time.monotonic()))
        status = "ok"
    except (asyncio.CancelledError, Exception) as exc:
        result = None
        status = str(exc) if isinstance(exc, (Unavailable, StorageConfigurationError)) else type(exc).__name__
        logger.info("CHSDK remote GATK attempt=%s stopped: %s", spec.container_name, status,
                    exc_info=not isinstance(exc, (Unavailable, asyncio.CancelledError)))
    return result, status, time.monotonic() - started


def fingerprint(path, deadline):
    """Verify indexed retrieval against a complete sequential read of this VCF."""
    import pysam

    # TBI's finest genomic bin is 16 KiB. Query occupied bins by position:
    # an index for different positions on the same chromosome can still
    # reproduce a whole-contig scan from the initial BGZF block.
    width = 1 << 14

    def window(row):
        chrom, pos, _ = row.split(b"\t", 2)
        return chrom.decode(), ((int(pos) - 1) // width) * width

    def update(digest, row):
        if time.monotonic() >= deadline:
            raise TimeoutError()
        digest.update(row.rstrip(b"\r\n") + b"\n")

    records, retrieved = hashlib.sha256(), hashlib.sha256()
    contigs = set()
    with gzip.open(path, "rb") as stream, pysam.TabixFile(str(path), index=str(path) + ".tbi") as indexed:
        for header in stream:
            if time.monotonic() >= deadline:
                raise TimeoutError()
            if header.startswith(b"#CHROM\t"):
                break
            if not header.startswith(b"##"):
                raise Unavailable("invalid VCF header")
        else:
            raise Unavailable("missing VCF columns")
        for (chrom, start), rows in groupby(stream, key=window):
            contigs.add(chrom)
            for row in rows:
                update(records, row)
            for row in indexed.fetch(chrom, start, start + width):
                # A deletion starting in the previous bin may overlap this one.
                if int(row.split("\t", 2)[1]) > start:
                    update(retrieved, row.encode())
        if contigs != set(indexed.contigs) or retrieved.digest() != records.digest():
            raise Unavailable("index disagrees with VCF")
    return header.rstrip(b"\r\n"), records.digest()


def compare(directory, output, deadline):
    with directory:
        local = fingerprint(output, deadline)
        remote = fingerprint(Path(directory.name) / output.name, deadline)
        return "match" if local == remote else "mismatch"
