import logging
from datetime import date, datetime, timedelta

import s3fs

from masters_project.utils import measure_memory, time_track

logger = logging.getLogger(__name__)


class GoesS3Client:
    def __init__(self, product_name: str, bucket_name: str, anon: bool = True) -> None:
        self.product_name = product_name
        self.bucket_name = bucket_name
        self.fs = s3fs.S3FileSystem(anon=anon)
        logger.info(f"GOES S3 Client created: {self}")

    def __repr__(self):
        return (
            f"Product: {self.product_name}, Bucket: {self.bucket_name}, FS: {self.fs}"
        )

    @measure_memory
    @time_track
    def get_files_path(
        self,
        channel: str,
        start_date: date | str,
        end_date: date | str = None,
    ):
        """
        Fetches GOES S3 file paths for a continuous date range.
        Accepts datetime.date objects or strings like 'YYYY-MM-DD'.
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
                logger.info(
                    f"Date: {current_date} (Julian: {day_of_year:03d}), "
                    f"Channel {channel} -> Found {len(found)} files."
                )
                all_paths.extend(found)
            except Exception as e:
                logger.error(f"Error on {current_date}, Channel {channel}: {e}")

        logger.info(f"Search complete. Total files collected: {len(all_paths)}")

        return all_paths

    @measure_memory
    @time_track
    def get_file(self, file_path: str) -> s3fs.core.S3File:
        uri = f"s3://{file_path}"
        logger.info(f"Opening S3 stream for: {uri}")
        try:
            return self.fs.open(uri)
        except Exception as e:
            logger.error(f"Failed to open S3 file: {uri}. Error: {e}")
            raise
