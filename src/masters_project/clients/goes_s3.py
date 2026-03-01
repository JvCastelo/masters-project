import logging
from datetime import date, datetime, timedelta

import s3fs

from masters_project.utils import measure_memory, time_track

logger = logging.getLogger(__name__)


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

    @measure_memory
    @time_track
    def get_files_path(
        self,
        channel: str,
        start_date: date | str,
        end_date: date | str | None = None,
    ) -> list[str]:
        """Fetch GOES S3 file paths for a continuous date range and channel.

        Accepts datetime.date objects or ISO date strings ('YYYY-MM-DD'). Iterates
        day-by-day and globs NetCDF files matching the product, year, day-of-year,
        and channel pattern.

        Args:
            channel: Channel identifier used in the file name (e.g., 'C01', 'C02').
            start_date: Start of the date range. Either a date object or
                'YYYY-MM-DD' string.
            end_date: End of the date range (inclusive). Either a date object or
                'YYYY-MM-DD' string. If None, defaults to start_date.

        Returns:
            A list of S3 object key paths (strings) for matching NetCDF files.

        Raises:
            ValueError: If start_date is after end_date.
        """
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

        if end_date is None:
            end_date = start_date
        elif isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        logger.info(f"Initiating search from {start_date} to {end_date} for {channel}.")

        if start_date > end_date:
            logger.error(
                f"Search failed: start_date ({start_date}) > end_date ({end_date})."
            )
            raise ValueError("start_date cannot be after end_date.")

        all_paths = []

        total_days = (end_date - start_date).days

        for i in range(total_days + 1):
            current_date = start_date + timedelta(days=i)
            year = current_date.year
            day_of_year = current_date.timetuple().tm_yday

            pattern = f"{self.bucket_name}/{self.product_name}/{year}/{day_of_year:03d}/*/*{channel}*.nc"

            try:
                found = self.fs.glob(pattern)
                logger.debug(
                    f"Date: {current_date} (Julian: {day_of_year:03d}), "
                    f"Channel {channel} -> Found {len(found)} files."
                )
                all_paths.extend(found)
            except Exception:
                logger.exception(f"Error on {current_date}, Channel {channel}")

        logger.info(f"Search complete. Total files collected: {len(all_paths)}")

        return all_paths

    @measure_memory
    @time_track
    def get_file(self, file_path: str) -> s3fs.core.S3File:
        """Open a single GOES file from S3 as a file-like stream.

        Args:
            file_path: S3 object key (path within the bucket), without the
                's3://' prefix.

        Returns:
            An s3fs file-like object (S3File) opened for reading.

        Raises:
            Exception: Re-raises any exception from s3fs when opening the object
                (e.g., missing key, network or permission errors).
        """
        uri = f"s3://{file_path}"
        logger.debug(f"Opening S3 stream for: {uri}")
        try:
            return self.fs.open(uri)
        except Exception:
            logger.exception(f"Failed to open S3 file: {uri}")
            raise
