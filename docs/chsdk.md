# ComputeHorde SDK runner

Minos defaults to `CONTAINER_RUNNER=native`. To opt in,
install the CHSDK below, then set this single setting in the validator's `.env`:

```dotenv
CONTAINER_RUNNER=chsdk
```

Each container invocation runs the real SN107 payload through the SDK's local
Docker runner and concurrently submits `hello-world:latest` to Compute Horde
(SN12) through `https://facilitator.computehorde.io/`. Both are awaited before
returning the local result (or raising the local error). Remote failures are
logged and do not fail the local job. Remote submission and polling are limited
to 300 seconds, capped by the local invocation's remaining timeout; closing the
remote HTTP client has a separate five-second limit. A polling timeout does not
cancel the already submitted remote job.

The remote job uses namespace `SN107.0` and executor class
`always_on.cpu.8c.16gb`, with download/execution/upload limits of 30/10/10 seconds.
Submission, job UUID, status changes, output, and completion are logged through
`templates._chsdk` (INFO for progress, WARNING for failures). Remote output is
kept out of the local payload's stdout/stderr and output files.

Requests are signed using the validator's `WALLET_NAME` / `WALLET_HOTKEY`
environment settings, falling back to `--wallet.name` / `--wallet.hotkey`
(both default to `default`). `--wallet.path` is also honored. Set
`CHSDK_VALIDATOR_HOTKEY` to the target Compute Horde validator's SS58 address;
if unset or empty, it defaults to
`5Hjbf5s2Cb4xuYGLKUGVEkwQrMpSMYvnatJxVenMnjodtEKi`.
No target validator wallet is needed. No additional wallet package pin is needed.

## Installation

Install `compute-horde-sdk` from PyPI together with the Minos stack.

```bash
python -m pip install -r requirements-chsdk.txt
python -m pip check
```

## Testing

Install `requirements-dev.txt` into the selected environment, then run:

```bash
python -m pytest
MINOS_CHSDK_DOCKER_TESTS=1 python -m pytest tests/test_chsdk_docker.py
```
