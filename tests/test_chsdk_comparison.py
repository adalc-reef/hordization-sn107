"""Comparison contracts with a fake facilitator/engine and disposable round data."""
import asyncio
import gzip
import io
import threading
import time
from contextvars import copy_context
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("compute_horde_sdk")
import pysam
from compute_horde_sdk.v1 import ComputeHordeJobStatus
from compute_horde_sdk.v1.local import LocalJobCancelledError
from templates import _chsdk, _chsdk_compare as comparison, _common
from templates._chsdk_storage import Store
from tests.test_chsdk import Engine, runtime
from tests.test_chsdk_gatk_docker import inputs
from tests.test_file_utils import CannedServer
from utils.path_utils import safe_round_dir_name



def vcf(path, gt="0/1", sample="sample", header="local", contig="chr20", position=10):
    text = path.with_suffix("")
    text.write_text(f"##fileformat=VCFv4.2\n##source={header}\n"
                    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t" + sample + "\n"
                    + f"{contig}\t{position}\t.\tA\tC\t60\tPASS\t.\tGT\t{gt}\n")
    pysam.tabix_compress(str(text), str(path), force=True)
    pysam.tabix_index(str(path), preset="vcf", force=True)


@pytest.mark.parametrize("source", ["primary", "backup", "built"])
def test_round_preparation_and_refresh(tmp_path, monkeypatch, source):
    from neurons import validator as validator_module

    ctx = comparison.ComparisonRound("test-round", tmp_path)
    bam = tmp_path / "platform" / "a.bam"
    ref = inputs(tmp_path, bam)
    routes = {"/reads": {"body": bam.read_bytes()}, "/truth": {"body": gzip.compress(b"chr20\t10\n")},
              "/truth-index": {"body": b"index"}}
    index_route = "/primary-index" if source == "primary" else "/different-backup-index"
    if source != "built":
        routes[index_route] = {"body": Path(str(bam) + ".bai").read_bytes()}
    server = CannedServer(routes)
    validator = object.__new__(validator_module.Validator)
    monkeypatch.setattr(validator_module, "BASE_DIR", tmp_path)
    monkeypatch.setenv("STORAGE_PRIMARY_BACKEND", "aws_s3")
    data = dict(region="chr20:1-100", bam_presigned_url=server.url("/missing-bam"),
                bam_presigned_url_backup=server.url("/reads"), bam_index_presigned_url=server.url("/primary-index"),
                bam_index_presigned_url_backup=server.url("/different-backup-index"),
                truth_vcf_presigned_url=server.url("/truth"), truth_vcf_index_presigned_url=server.url("/truth-index"))
    try:
        # A later preparation step fails. Retry with fresh links; keep the
        # BAM cache and the identity of its successful backup.
        assert validator._download_round_files(ctx.round_id, data, ctx) is None
        data["bam_presigned_url_backup"] += "?X-Amz-Signature=renewed"
        data["mutations_vcf_presigned_url"] = server.url("/truth")
        result = validator._download_round_files(ctx.round_id, data, ctx)
        assert result["ref_path"] == ref
        assert ctx.urls["round.bam"] == data["bam_presigned_url_backup"]
        assert server.hits.count(("GET", "/reads")) == 1
        assert ctx.built_index == (source == "built")
        if source != "built":
            assert ctx.urls["round.bam.bai"] == server.url(index_route)
        else:
            assert ctx.urls.get("round.bam.bai") is None
            with pysam.AlignmentFile(ctx.bam) as reads:
                assert reads.count("chr20") == 60
        fresh = comparison.ComparisonRound(ctx.round_id, tmp_path)
        assert validator._download_round_files(ctx.round_id, data, fresh)
        assert fresh.urls["round.bam"] is None
    finally:
        server.close()


class Blob(io.BytesIO):
    def iter_chunks(self, size):
        yield self.read()


class MemoryStore(Store):
    def __init__(self):
        self.objects = {}
        self.events = []
        self.bucket = "test"
        self.client = self
        self.block_upload = threading.Event()
        self.block_upload.set()
        self.uploading = threading.Event()

    def upload_index(self, key, path):
        self.events.append(("upload", key))
        self.uploading.set()
        assert self.block_upload.wait(5)
        self.objects[key] = path.read_bytes()

    def delete(self, key):
        self.events.append(("delete", key))
        self.objects.pop(key, None)

    def get_object(self, Bucket, Key):
        self.events.append(("get", Key))
        return {"Body": Blob(self.objects[Key])}

    def sign(self, operation, key, deadline):
        return "https://storage.test/" + key


@pytest.fixture
def pair(runtime, tmp_path, monkeypatch, caplog):
    runner, queued, _, launch, _ = runtime
    caplog.set_level("DEBUG", logger=_chsdk.__name__)
    ctx = comparison.ComparisonRound("round", tmp_path)
    bam = tmp_path / "output" / "scoring" / safe_round_dir_name(ctx.round_id) / "round.bam"
    ref = inputs(tmp_path, bam)
    ctx.record_source(bam, "https://platform.test/accepted")
    ctx.built_index = True
    region = "chr20:1-100"
    _common.begin_round()
    _chsdk.bind_round(ctx, ref, region, None)
    store = MemoryStore()
    monkeypatch.setattr(comparison, "Store", lambda: store)
    monkeypatch.setattr(_chsdk, "_remote_keys", lambda: ("signer", "target"))
    state = SimpleNamespace(specs=[], status=ComputeHordeJobStatus.COMPLETED, genotype="0/1",
                            submitted=threading.Event(), stalled=None)
    outdir = tmp_path / "remote"
    outdir.mkdir()

    class Client:
        result = None
        error = None

        def __init__(self, **kwargs):
            self._client = self

        async def aclose(self):
            pass

        async def create_job(self, spec):
            state.specs.append(spec)
            self.uuid = f"test-job-{len(state.specs)}"
            state.submitted.set()
            if state.stalled == "submission":
                await asyncio.sleep(60)
            for dest, vol in spec.output_volumes.items():
                key = vol.url.removeprefix("https://storage.test/")
                path = outdir / Path(dest).name
                if not dest.endswith(".tbi"):
                    vcf(path, gt=state.genotype, header="remote")
                store.objects[key] = path.read_bytes()
            return self

        @property
        def status(self):
            return state.status

        async def wait(self, status_callback):
            status_callback(self, SimpleNamespace(status=self.status))
            if state.stalled == "wait":
                await asyncio.sleep(60)

    monkeypatch.setattr(comparison, "ComputeHordeClient", Client)

    def call(miner="full_hotkey", timeout=15):
        output = ctx.bam.parent / f"miner_{miner}.vcf.gz"
        vcf(output)
        cmd = ["docker", "run", "--rm", "--name", _common.container_name("gatk"),
               "-v", f"{ctx.bam.parent}:/data/bams:ro", "-v", f"{ref.parent}:/data/reference:ro",
               "-v", f"{output.parent}:/data/output", "broadinstitute/gatk:4.5.0.0",
               "gatk", "--java-options", "-Xmx512m", "HaplotypeCaller", "-I", "/data/bams/round.bam",
               "-R", "/data/reference/chr20.fa", "-O", "/data/output/" + output.name, "-L", region]
        return launch(cmd, timeout=timeout)

    yield ctx, store, state, call
    _chsdk.finish_round(ctx)
    # Quiesce the deliberately abandoned fake facilitator's expiry sweeps.
    async def drain():
        for task in list(comparison._background):
            if task.get_loop() == runner.loop:
                task.cancel()
        await asyncio.sleep(0)
    runner.call(drain())


def test_concurrent_index_retries_and_per_file_deletion(pair, caplog, monkeypatch):
    ctx, store, state, call = pair
    store.block_upload.clear()
    entrants = []
    both_waiting = threading.Event()
    index_url = ctx.index_url

    async def enter(*args):
        entrants.append(asyncio.current_task())
        if len(entrants) == 2:
            both_waiting.set()
        return await index_url(*args)

    monkeypatch.setattr(ctx, "index_url", enter)
    first, second = call("one"), call("two")
    assert both_waiting.wait(2)
    assert store.uploading.wait(2)
    store.block_upload.set()
    assert [f.result(3).returncode for f in (first, second)] == [0, 0]
    assert call("one").result(3).returncode == 0
    assert len(state.specs) == 3
    keys = []
    for spec in state.specs:
        assert {mount: (vol.cache_key, vol.cache_ttl_seconds) for mount, vol in spec.input_volumes.items()} == {
            "/volume/data/round.bam": (f"output/scoring/{safe_round_dir_name(ctx.round_id)}/round.bam", 7200),
            "/volume/data/round.bam.bai": (f"output/scoring/{safe_round_dir_name(ctx.round_id)}/round.bam.bai", 7200),
            "/volume/reference/chr20.fa": ("datasets/reference/chr20/chr20.fa", 604800),
            "/volume/reference/chr20.fa.fai": ("datasets/reference/chr20/chr20.fa.fai", 604800),
            "/volume/reference/chr20.dict": ("datasets/reference/chr20/chr20.dict", 604800),
        }
        assert spec.input_volumes["/volume/data/round.bam.bai"].url.endswith(ctx.index_key)
        vcf_key, index_key = [vol.url.removeprefix("https://storage.test/") for vol in spec.output_volumes.values()]
        assert store.events.index(("delete", vcf_key)) < store.events.index(("get", index_key))
        keys.extend([vcf_key, index_key])
    assert len(set(keys)) == 6
    assert caplog.text.count("result=match") == 3
    assert list(store.objects) == [ctx.index_key]
    assert store.events.count(("upload", ctx.index_key)) == 1
    _chsdk.finish_round(ctx)
    _chsdk.finish_round(ctx)
    assert not store.objects
    assert store.events.count(("delete", ctx.index_key)) == 1


def test_downloaded_index_and_local_outcomes(pair, runtime, caplog):
    ctx, store, state, call = pair
    ctx.built_index = False
    ctx.urls["round.bam.bai"] = "https://platform.test/index/separate?Signature=secret"
    state.genotype = "1/1"
    local = call().result(2)
    assert local.returncode == 0 and local.stdout == b"stdout\r\n"
    assert "result=mismatch" in caplog.text
    index = state.specs[-1].input_volumes["/volume/data/round.bam.bai"]
    assert index.url == ctx.urls["round.bam.bai"]
    assert index.cache_key == f"output/scoring/{safe_round_dir_name(ctx.round_id)}/round.bam.bai"
    assert index.cache_ttl_seconds == 7200
    assert not any(op == "upload" for op, _ in store.events)
    state.status = ComputeHordeJobStatus.REJECTED
    assert call().result(2).returncode == 0
    assert "result=unavailable reason=remote job rejected" in caplog.text
    state.status = ComputeHordeJobStatus.COMPLETED
    runtime[1].append(Engine(returncode=23))
    assert call().result(2).returncode == 23
    assert "local=exit 23" in caplog.text
    engine = Engine()
    engine.create_error = OSError("test launch failure")
    runtime[1].append(engine)
    with pytest.raises(Exception, match="test launch failure"):
        call().result(2)


def test_bounded_wait_and_late_upload_retirement(pair, runtime):
    ctx, store, _, call = pair
    store.block_upload.clear()
    runtime[1].append(Engine(blocked=("logs",)))
    future = call()
    assert store.uploading.wait(2)
    assert _common.reap_live_containers(0) in (0, 1)
    assert ctx.closed and not ctx.close_task.done()
    store.block_upload.set()
    _chsdk.finish_round(ctx)
    assert not store.objects
    with pytest.raises(LocalJobCancelledError):
        future.result(2)
    with pytest.raises(_common.RegistryClosed):
        call().result(2)


@pytest.mark.parametrize("stage", ["submission", "wait", "retrieval"])
def test_submission_and_retrieval_share_deadline(pair, caplog, monkeypatch, stage):
    ctx, store, state, call = pair
    state.stalled = stage
    release_transfer = threading.Event()
    if stage == "retrieval":
        retrieve = store.retrieve

        def blocked(*args):
            assert release_transfer.wait(8)
            return retrieve(*args)
        monkeypatch.setattr(store, "retrieve", blocked)
    ctx.deadline = time.monotonic() + 3.8
    start = time.monotonic()
    try:
        assert call(timeout=30).result(5).returncode == 0
    finally:
        release_transfer.set()
    assert state.submitted.is_set()
    assert time.monotonic() - start < 4.8
    assert "TimeoutError" in caplog.text
    _common.reap_live_containers()
    assert not store.objects


def test_legacy_and_unknown_bam_sources(pair, caplog):
    ctx, _, state, call = pair
    ref = ctx.reference
    ctx.reference = ref.parent.parent / ref.name
    assert call().result(2).returncode == 0
    ctx.reference = ref
    ctx.record_source(ctx.bam, None)
    assert call().result(2).returncode == 0
    assert not state.specs
    assert caplog.text.count("result=unavailable") == 2


def test_sample_identity_and_index(tmp_path):
    a, b = (tmp_path / f"{stem}.vcf.gz" for stem in "ab")
    deadline = time.monotonic() + 10
    vcf(a)
    vcf(b, sample="other")
    assert comparison.fingerprint(a, deadline) != comparison.fingerprint(b, deadline)
    vcf(b, position=90000)
    Path(str(a) + ".tbi").write_bytes(Path(str(b) + ".tbi").read_bytes())
    with pytest.raises((comparison.Unavailable, ValueError)):
        comparison.fingerprint(a, deadline)


def test_partial_retrieval_and_cleanup_retry(pair, caplog, monkeypatch):
    ctx, store, _, call = pair
    original_get = store.get_object
    original_delete = store.delete
    transient = []

    def fail_index_get(Bucket, Key):
        if Key.endswith(".tbi"):
            assert ("delete", Key.removesuffix(".tbi")) in store.events
            raise OSError("URL with secret marker")
        return original_get(Bucket, Key)

    def flaky_delete(key):
        if key.endswith(".tbi") and not transient:
            transient.append(key)
            raise OSError("URL with secret marker")
        original_delete(key)

    monkeypatch.setattr(store, "get_object", fail_index_get)
    monkeypatch.setattr(store, "delete", flaky_delete)
    assert call().result(2).returncode == 0
    _chsdk.finish_round(ctx)
    assert transient and not store.objects
    assert "result=unavailable reason=retrieving miner_full_hotkey.vcf.gz.tbi failed (OSError)" in caplog.text


def test_other_invocations_never_enter_comparison(pair, runtime, monkeypatch):
    _, _, state, _ = pair
    seen = []

    async def unexpected(*args):
        seen.append(args)
        raise AssertionError("unexpected remote comparison")

    monkeypatch.setattr(comparison, "run_remote", unexpected)
    for image, args in (("bcftools", ["sh", "-lc", "bcftools call"]),
                        ("freebayes", ["freebayes"]), ("deepvariant", ["run_deepvariant"]),
                        ("samtools", ["samtools", "index"]), ("happy", ["/truth/truth.vcf", "/query.vcf"]),
                        ("gatk", ["gatk", "IndexFeatureFile"])):
        command = ["docker", "run", "--rm", "--name", _common.container_name("helper"), image, *args]
        assert runtime[3](command).result(2).returncode == 0
    assert not seen and not state.specs


def test_late_put_after_interrupted_wait(pair, runtime):
    ctx, store, state, call = pair
    ctx.deadline = time.monotonic() + 3.5
    state.stalled = "wait"
    runtime[1].append(Engine(blocked=("logs",)))
    running = call()
    assert state.submitted.wait(2)
    # The worker's PUTs can still complete after the initial deletion.
    _common.reap_live_containers(.1)
    with pytest.raises(LocalJobCancelledError):
        running.result(2)
    keys = [v.url.removeprefix("https://storage.test/") for v in state.specs[0].output_volumes.values()]
    store.objects.update(dict.fromkeys(keys, b"late PUT"))
    assert ctx.index_key in store.objects

    async def settled():
        sweeps = [task for task in comparison._background if task.get_loop() == runtime[0].loop]
        await asyncio.wait_for(asyncio.gather(*sweeps), 8)
    runtime[0].call(settled())
    assert not store.objects


def test_delayed_watchdog_preserves_new_generation(pair):
    ctx, _, _, _ = pair
    previous = copy_context()
    _chsdk.finish_round(ctx)
    new = _common.begin_round()
    assert previous.run(_common.reap_live_containers) == 0
    assert _common._open_round == new
