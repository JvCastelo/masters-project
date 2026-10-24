import json
import logging
import re
from collections import defaultdict
from collections.abc import Collection
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import s3fs

from masters_project.utils import time_track

logger = logging.getLogger(__name__)

# Measured on C03/C13 (2022-02-01): 512 KB blocks read ~10% of a Full Disk file in ~8 requests.
# ``cache_type="none"`` makes ~150 tiny requests, and the s3fs default can read far more than the file.
PARTIAL_BLOCK_SIZE = 512 * 1024

# "OR_ABI-L1b-RadF-M6C03_G16_s..." -> "C03" (the scan mode digit varies: M3, M4, M6).
CHANNEL_PATTERN = re.compile(r"-M\d+(C\d{2})_")


class GoesS3Client:
    """Client for listing and opening GOES (Geostationary Operational Environmental Satellite) data files from AWS S3.

    Wraps s3fs to access GOES-R series products stored in the NOAA Open Data Registry
    bucket structure. Supports anonymous access and date-range file discovery by channel.
    """

    def __init__(self, product_name: str, bucket_name: str, anon: bool = True) -> None:
        """Initialize the GOES S3 client.

        Args:
            product_name: GOES product identifier (e.g., 'ABI-L1b-RadC').
            bucket_name: S3 bucket name hosting the GOES data (e.g., 'noaa-goes16').
            anon: If True, use anonymous access; if False, use configured AWS credentials.
                  Defaults to True.
        """
        self.product_name = product_name
        self.bucket_name = bucket_name
        self.fs = s3fs.S3FileSystem(anon=anon)
        logger.debug(f"GOES S3 Client created: {self}")

    def __repr__(self) -> str:
        """Return a string representation of the client for debugging.

        Returns:
            A string containing product name, bucket name, and filesystem info.
        """
        return (
            f"Product: {self.product_name}, Bucket: {self.bucket_name}, FS: {self.fs}"
        )

    def open_partial(self, file_path: str) -> s3fs.core.S3File:
        """Open a GOES file for partial reads (HTTP range requests with a block cache).

        Unlike :meth:`get_file` followed by ``read()``, this does not download the whole
        file: h5netcdf only fetches the metadata blocks and the chunks it actually touches.
        Intentionally not decorated with ``measure_memory``/``time_track``: ``tracemalloc``
        slows down the hot path and is not safe across threads.

        Args:
            file_path: S3 object key (path within the bucket), without the 's3://' prefix.

        Returns:
            An s3fs file-like object (S3File). Callers must close it.
        """
        return self.fs.open(
            f"s3://{file_path}",
            cache_type="blockcache",
            block_size=PARTIAL_BLOCK_SIZE,
        )

    def list_day(self, day: date) -> list[str]:
        """List every ``.nc`` key (all channels) of one day in a single call.

        Args:
            day: Calendar day to list.

        Returns:
            Sorted S3 keys without the 's3://' prefix.
        """
        day_of_year = day.timetuple().tm_yday
        pattern = f"{self.bucket_name}/{self.product_name}/{day.year}/{day_of_year:03d}/*/*.nc"
        return sorted(self.fs.glob(pattern))

    def _list_day_cached(self, day: date, cache_dir: Path | None) -> list[str]:
        """List one day, reusing an on-disk listing for days that can no longer change.

        Only days older than yesterday (UTC) are cached, and never an empty or failed
        listing, so a transient S3 error cannot be remembered as "no files".
        """
        cache_file = None
        if cache_dir is not None:
            cache_file = (
                cache_dir / str(day.year) / f"{day.timetuple().tm_yday:03d}.json"
            )
            if cache_file.exists():
                return json.loads(cache_file.read_text())

        try:
            keys = self.list_day(day)
        except Exception:
            logger.exception(f"Error listing {day}")
            return []

        closed_day = day < datetime.now(UTC).date() - timedelta(days=1)
        if cache_file is not None and keys and closed_day:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            tmp_file = cache_file.with_suffix(".json.tmp")
            tmp_file.write_text(json.dumps(keys))
            tmp_file.replace(cache_file)
        return keys

    @time_track
    def list_files_by_channel(
        self,
        channels: list[str],
        start_date: date | str,
        end_date: date | str,
        max_workers: int = 16,
        cache_dir: Path | None = None,
        only_days: Collection[date] | None = None,
    ) -> dict[str, list[str]]:
        """List the files of several channels for a date range with one call per day.

        Days are listed in parallel (it is network latency, not CPU) and the file names
        are split by channel, instead of one ``glob`` per channel and day.

        Args:
            channels: Channel identifiers to keep (e.g. ['C01', 'C03']).
            start_date: Start of the range (date or 'YYYY-MM-DD').
            end_date: End of the range, inclusive (date or 'YYYY-MM-DD').
            max_workers: Threads used to list days concurrently.
            cache_dir: Directory for per-day listing caches; None disables caching.
            only_days: If given, only these days (within the range) are listed; days
                outside it are not even requested from S3.

        Returns:
            Mapping channel -> sorted S3 keys. Channels without files map to [].

        Raises:
            ValueError: If start_date is after end_date.
        """
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        if start_date > end_date:
            raise ValueError("start_date cannot be after end_date.")

        days = [
            start_date + timedelta(days=i)
            for i in range((end_date - start_date).days + 1)
        ]
        if only_days is not None:
            days = [d for d in days if d in only_days]
        logger.info(f"Listing {len(days)} days for channels {channels}.")

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            listings = list(
                pool.map(lambda d: self._list_day_cached(d, cache_dir), days)
            )

        wanted = set(channels)
        by_channel: dict[str, list[str]] = defaultdict(list)
        for keys in listings:
            for key in keys:
                match = CHANNEL_PATTERN.search(key.rsplit("/", 1)[-1])
                if match and match.group(1) in wanted:
                    by_channel[match.group(1)].append(key)

        result = {channel: sorted(by_channel.get(channel, [])) for channel in channels}
        logger.info(
            "Search complete. Files per channel: "
            + ", ".join(f"{c}={len(f)}" for c, f in result.items())
        )
        return result
