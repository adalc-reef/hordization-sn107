"""Chromosome FASTAs and sidecars installed by Minos."""

# Supported chromosomes
SUPPORTED_CHROMOSOMES = [f"chr{i}" for i in range(1, 23)]  # chr1-chr22
# Permanent indirected URL — platform 302-redirects to the actual storage
# backend (R2 today, anything tomorrow). Lets us swap providers without
# requiring miner/validator setup-script updates.
REF_S3_BASE = "https://api.theminos.ai/reference"

# Reference data files per role — multi-chromosome (chr1-chr22)
# Estimated sizes in MB for user feedback
def _build_miner_data_files():
    files = []
    for chrom in SUPPORTED_CHROMOSOMES:
        files.extend([
            {
                "name": f"GRCh38 {chrom} Reference",
                "local": f"datasets/reference/{chrom}/{chrom}.fa",
                "url": f"{REF_S3_BASE}/{chrom}/{chrom}.fa",
                "size_mb": 60,
            },
            {
                "name": f"{chrom} Reference Index",
                "local": f"datasets/reference/{chrom}/{chrom}.fa.fai",
                "url": f"{REF_S3_BASE}/{chrom}/{chrom}.fa.fai",
                "size_mb": 1,
            },
            {
                "name": f"{chrom} Reference Dictionary",
                "local": f"datasets/reference/{chrom}/{chrom}.dict",
                "url": f"{REF_S3_BASE}/{chrom}/{chrom}.dict",
                "size_mb": 1,
            },
        ])
    return files

MINER_DATA_FILES = _build_miner_data_files()
