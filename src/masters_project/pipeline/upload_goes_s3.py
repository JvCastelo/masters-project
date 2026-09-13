"""Upload raw GOES CSVs to S3 under hive-style partitions, skipping existing keys."""

import logging
import re
import time
from pathlib import Path

from tqdm import tqdm

from masters_project.clients.goes_upload_s3 import GoesUploadClient
from masters_project.settings import settings

settings.setup_logging("upload_goes_s3")
logger = logging.getLogger(__name__)

# Matches the canonical name produced by FilePathBuilder.raw_goes:
#   goes_{channel}_st_{YYYY-MM-DD}_et_{YYYY-MM-DD}_{station}.csv
# Anchoring on the fixed et_ date lets the station group hold underscores
# (e.g. SAO_MARTINHO_DA_SERRA).
GOES_FILENAME_PATTERN = re.compile(
    r"^goes_(C\d{2})_st_(\d{4})-(\d{2})-\d{2}_et_\d{4}-\d{2}-\d{2}_(.+)\.csv$"
)


def _resolve_source_dir() -> Path:
    """Return the directory to scan for GOES CSVs.

    Uses ``upload_config.source_dir`` when set; otherwise defaults to the
    pipeline's own output directory (``data/raw/goes``).
    """
    configured = settings.upload.source_dir
    if configured:
        return Path(configured)
    return settings.RAW_PATH / "goes"


def _build_s3_key(filename: str) -> str | None:
    """Map a GOES CSV filename to its partitioned S3 key.

    Args:
        filename: Base filename (no directory component).

    Returns:
        Key ``estacao={station}/canal={channel}/ano={year}/mes={month}/{filename}``,
        or None if the name does not match the expected GOES pattern.
    """
    match = GOES_FILENAME_PATTERN.match(filename)
    if not match:
        return None

    channel, year, month, station = match.groups()
    return f"estacao={station}/canal={channel}/ano={year}/mes={month}/{filename}"


def main() -> None:
    """Scan the source directory, skip already-uploaded keys, and upload the rest."""
    source_dir = _resolve_source_dir()

    if not source_dir.is_dir():
        logger.error(f"Source directory not found: {source_dir}")
        return

    logger.info(f"Scanning {source_dir} for GOES CSVs...")

    pending: list[tuple[Path, str]] = []
    for path in sorted(source_dir.rglob("goes_*.csv")):
        key = _build_s3_key(path.name)
        if key is None:
            logger.warning(f"Filename did not match GOES pattern, skipping: {path.name}")
            continue
        pending.append((path, key))

    if not pending:
        logger.warning("No GOES CSVs matched the expected naming pattern. Nothing to do.")
        return

    client = GoesUploadClient(bucket_name=settings.upload.bucket_name)
    existing_keys = client.list_existing_keys()

    to_upload = [(p, k) for p, k in pending if k not in existing_keys]
    skipped = len(pending) - len(to_upload)

    logger.info(
        f"{len(pending)} local files, {skipped} already in bucket, "
        f"{len(to_upload)} to upload."
    )

    if not to_upload:
        logger.info("Bucket already up to date. Nothing to upload.")
        return

    start = time.perf_counter()
    uploaded, failures = 0, 0

    for local_path, key in tqdm(to_upload, desc="Uploading to S3"):
        size_mb = local_path.stat().st_size / (1024 * 1024)
        try:
            client.upload(local_path, key)
            uploaded += 1
            logger.info(f"OK ({size_mb:.2f} MB) -> {key}")
        except Exception:
            failures += 1
            logger.error(f"FAILED -> {key}")

    duration = time.perf_counter() - start
    logger.info(
        f"Done: {uploaded} uploaded, {failures} failed, {skipped} skipped, "
        f"{duration:.1f}s"
    )


if __name__ == "__main__":
    main()
