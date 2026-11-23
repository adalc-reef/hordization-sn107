"""Disposable GATK calls through the real v1 serializer, Docker and a local S3 emulator."""
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import os
import random
import shutil
import socket
import subprocess
import time

import pytest

pytest.importorskip("compute_horde_sdk")
from templates import _chsdk, _chsdk_compare, _common
from utils.path_utils import safe_round_dir_name

pytestmark = pytest.mark.skipif(
    os.getenv("MINOS_CHSDK_DOCKER_TESTS") != "1" or not shutil.which("moto_server"),
    reason="Needs disposable Docker tests enabled and moto_server on PATH",
)


@pytest.fixture
def endpoint():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    with subprocess.Popen(["moto_server", "-H", "127.0.0.1", "-p", str(port)],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) as process:
        try:
            for _ in range(100):
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=.1):
                        break
                except OSError:
                    time.sleep(.1)
            yield f"http://127.0.0.1:{port}"
        finally:
            process.terminate()


def inputs(base, bam):
    import pysam

    sequence = "".join(random.Random(107).choices("ACGT", k=1000))
    ref = base / "datasets/reference/chr20/chr20.fa"
    ref.parent.mkdir(parents=True)
    ref.write_text(">chr20\n" + sequence + "\n")
    pysam.faidx(str(ref))
    ref.with_suffix(".dict").write_text("@HD\tVN:1.6\n@SQ\tSN:chr20\tLN:1000\n")
    bam.parent.mkdir(parents=True)
    header = {"HD": {"VN": "1.6", "SO": "coordinate"}, "SQ": [{"SN": "chr20", "LN": 1000}],
              "RG": [{"ID": "reads", "SM": "sample", "PL": "ILLUMINA"}]}
    with pysam.AlignmentFile(bam, "wb", header=header) as out:
        for i in range(60):
            start = 300 + i
            bases = list(sequence[start:start + 150])
            if i % 3:
                bases[400 - start] = "C" if sequence[400] == "A" else "A"
            read = pysam.AlignedSegment(out.header)
            read.query_name = f"read{i}"
            read.reference_id, read.reference_start, read.mapping_quality = 0, start, 60
            read.cigarstring = "150M"
            read.query_sequence = "".join(bases)
            read.query_qualities = [40] * 150
            read.set_tag("RG", "reads")
            out.write(read)
    pysam.index(str(bam))
    return ref


@pytest.mark.parametrize("scoring_error", [False, True])
def test_pair_with_http_volumes(tmp_path, monkeypatch, caplog, endpoint, scoring_error):
    import boto3
    import pysam
    from neurons import validator as deployed
    from compute_horde_core.output_upload import OutputUploader, SingleFilePutUpload
    from compute_horde_core.volume import VolumeDownloader, SingleFileVolume
    from bittensor_wallet import Keypair

    monkeypatch.setenv("CHSDK_S3_ACCESS_KEY_ID", "disposable-access")
    monkeypatch.setenv("CHSDK_S3_SECRET_ACCESS_KEY", "disposable-secret")
    monkeypatch.setenv("CHSDK_S3_BUCKET", "comparison")
    monkeypatch.setenv("CONTAINER_RUNNER", "chsdk")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", endpoint)  # route boto3 only to moto
    caplog.set_level("DEBUG", logger=_chsdk.__name__)
    storage = boto3.client("s3", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
    storage.create_bucket(Bucket="comparison")
    bam = tmp_path / "platform" / "source.bam"
    inputs(tmp_path, bam)
    truth = bam.with_name("truth.vcf")
    truth.write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
                     "chr20\t401\t.\tA\tC\t60\tPASS\t.\n")
    truth_gz = Path(pysam.tabix_index(str(truth), preset="vcf"))

    def publish(path):
        storage.put_object(Bucket="comparison", Key=path.name, Body=path.read_bytes())
        return storage.generate_presigned_url("get_object", Params={"Bucket": "comparison", "Key": path.name})

    for entry in _chsdk_compare.MINER_DATA_FILES:
        path = tmp_path / entry["local"]
        if path.is_file():
            monkeypatch.setitem(entry, "url", publish(path))

    key = Keypair.create_from_mnemonic(Keypair.generate_mnemonic())
    monkeypatch.setattr(_chsdk, "_remote_keys", lambda: (key, key.ss58_address))
    worker = tmp_path / "worker"
    (worker / "output").mkdir(parents=True)
    miner = key.ss58_address
    filename = f"miner_{miner}.vcf.gz"
    region = "chr20:201-601"

    async def execute(body):
        volumes = body["volumes"]
        assert len(volumes) == 5
        assert {v["relative_path"] for v in volumes} == {
            "data/round.bam", "data/round.bam.bai", "reference/chr20.fa",
            "reference/chr20.fa.fai", "reference/chr20.dict",
        }
        for volume in volumes:
            assert volume["volume_type"] == "single_file"
            path = Path(volume["relative_path"])
            if path.parent == Path("data"):
                assert volume["cache_key"] == f"output/scoring/{safe_round_dir_name(round_id)}/{path.name}"
                assert volume["cache_ttl_seconds"] == 7200
            else:
                assert volume["cache_key"] == f"datasets/reference/chr20/{path.name}"
                assert volume["cache_ttl_seconds"] == 604800
            downloader = VolumeDownloader.for_volume(SingleFileVolume.model_validate(volume))
            await downloader.download(worker / "volume")
        args = body["args"]
        assert args == ["gatk", "--java-options", "-Xmx1638m", "HaplotypeCaller",
                        "-R", "/volume/reference/chr20.fa", "-I", "/volume/data/round.bam",
                        "-O", "/output/" + filename, "--native-pair-hmm-threads", "1",
                        "-L", region, "--min-base-quality-score", "12"]
        await asyncio.to_thread(subprocess.run, ["docker", "run", "--rm", "-v", f"{worker}/volume:/volume:ro",
                        "-v", f"{worker}/output:/output", body["docker_image"], *args],
                       check=True, capture_output=True, timeout=60)
        assert {upload["relative_path"] for upload in body["uploads"]} == {filename, filename + ".tbi"}

    async def facilitator(self, method, url, **kwargs):
        assert (method, url) == ("POST", "/api/v1/job-docker/")
        body = kwargs["json"]
        await execute(body)
        for upload in body["uploads"]:
            uploader = OutputUploader.for_upload_output(SingleFilePutUpload.model_validate(upload))
            await uploader.upload(worker / "output")
        stamp = "2026-01-01T00:00:00Z"
        return json.dumps(dict(body, uuid="test", created_at=stamp, stdout="", stderr="",
                               status_history=[dict(created_at=stamp, status="completed")]))

    monkeypatch.setattr(_chsdk_compare.ComputeHordeClient, "_make_request", facilitator)
    # Run the deployed preparation, generation binding, fan-out and finally.
    # Isolate the platform, hap.py and chain publication at their boundaries.
    round_id = "2026-01-21T12:00:00+00:00"
    submission = dict(miner_hotkey=miner, tool_name="gatk", tool_config={"gatk_options": {"min_base_quality_score": 12}})
    truth_url = publish(truth_gz)
    data = dict(region=region, submissions=[submission], bam_presigned_url=publish(bam),
                truth_vcf_presigned_url=truth_url, truth_vcf_index_presigned_url=publish(Path(str(truth_gz) + ".tbi")),
                mutations_vcf_presigned_url=truth_url)
    validator = object.__new__(deployed.Validator)
    validator.platform_client = SimpleNamespace(get_assignment=AsyncMock(return_value={}),
                                                get_round_submissions=AsyncMock(return_value=data),
                                                upload_file_to_s3=AsyncMock(return_value=True))
    validator.wallet = SimpleNamespace(hotkey=key)
    validator.is_registered = True
    validator._declared_scoring_version = AsyncMock(return_value=None)
    validator._adopt_round_pin = Mock()
    validator._score_tracker_snapshot = Mock(return_value={})
    validator._finalize_round_scores = AsyncMock(return_value=True)
    validator._scoring_cfg = dict(concurrency=1, threads_per_job=1, mem_per_job_gb=2)
    validator.happy_scorer = SimpleNamespace(score_vcf=Mock(return_value=None))
    monkeypatch.setattr(deployed, "BASE_DIR", tmp_path)
    monkeypatch.setattr(deployed, "VALIDATOR_VARIANT_CALLING_TIMEOUT_SECONDS", 60)
    if scoring_error:
        score_miner = validator._score_single_miner

        async def score_then_fail(*args, **kwargs):
            await score_miner(*args, **kwargs)
            raise RuntimeError("injected scoring-phase failure")

        validator._score_single_miner = score_then_fail
        validator._restore_score_tracker = Mock()
    assert asyncio.run(validator._score_round_submissions(round_id)) is (not scoring_error)
    assert caplog.text.count("result=match") == 1
    validator.happy_scorer.score_vcf.assert_called_once()
    authoritative = tmp_path / "output" / "scoring" / safe_round_dir_name(round_id) / filename
    assert validator.happy_scorer.score_vcf.call_args.kwargs["query_vcf"] == str(authoritative)
    assert _common.count_variants(authoritative) > 0
    assert storage.list_objects_v2(Bucket="comparison", Prefix="chsdk/")["KeyCount"] == 0
