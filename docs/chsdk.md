# Compute Horde GATK comparison

Minos defaults to the native container runner. To enable comparison, install
the optional runner and configure the validator's environment.

```bash
python -m pip install -r requirements-chsdk.txt
```

```dotenv
CONTAINER_RUNNER=chsdk
CHSDK_S3_BUCKET=<private bucket>
CHSDK_S3_ACCESS_KEY_ID=<access key>
CHSDK_S3_SECRET_ACCESS_KEY=<secret key>
```

The validator uses these explicit credentials for GetObject, PutObject and
DeleteObject under `chsdk/` and discovers the bucket region with HeadBucket.
The worker gets time-limited URLs for integration-owned objects.

Each scoring-round GATK HaplotypeCaller starts through the SDK's local Docker
runner. A concurrent Compute Horde job uses the same image and caller arguments.
Other callers, helper containers and hap.py run locally. Minos scores the local
VCF and preserves the local runner's result or exception.

The worker receives five files. The BAM and downloaded BAI use their accepted
platform URLs. A BAI built during preparation is uploaded once per round.
The FASTA, FAI and dictionary use the chromosome's installer inventory. A
cache populated before this preparation has unknown download provenance, and
a legacy or custom reference outside the standard inventory is unavailable
for comparison.

The job uses namespace `SN107.0` and executor class `always_on.cpu.4c.8gb`.
The caller timeout and assigned submit window bound the comparison. Without an
assignment, the caller timeout applies. At submission, about 20% of the remaining
seconds are allocated to input download, 10% to upload, and the balance to execution.

Requests are signed using the validator's `WALLET_NAME` and `WALLET_HOTKEY`,
falling back to `--wallet.name` and `--wallet.hotkey` (both default to
`default`). The runner also honors `--wallet.path`. The target validator
defaults to `5Hjbf5s2Cb4xuYGLKUGVEkwQrMpSMYvnatJxVenMnjodtEKi` and
can be selected with the existing `CHSDK_VALIDATOR_HOTKEY`.

Each attempt signs a distinct PUT for the VCF and its TBI. The validator
deletes each output immediately after retrieval. Round finalization retires
the shared index after outstanding uploads and any still-valid GET issued to
an ambiguously submitted job. The v1 remote SDK has no job cancellation API.
After an ambiguous submission or interrupted wait, Minos schedules another deletion
after the signed PUT expiry and upload window. These cleanups run in memory.
An S3 lifecycle expiration on the integration prefix can cover validator
process loss or persistent deletion failures.

The check compares the VCF column header and all decompressed record lines in order.
It also streams each VCF through its own tabix index and verifies that indexed
retrieval reproduces the sequential read.

## Verification

```bash
python -m pytest
MINOS_CHSDK_DOCKER_TESTS=1 python -m pytest tests/test_chsdk_docker.py tests/test_chsdk_gatk_docker.py
```

The GATK Docker test uses the real SDK serializer, input downloader and output
uploader, two GATK containers, and a loopback S3 emulator. It needs `moto_server` on PATH
(from the `moto[server]` test package).
