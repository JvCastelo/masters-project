import calendar
import logging
from datetime import datetime

import s3fs

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

    def get_files_path_by_year(
        self, start_year: int = 2017, end_year: int = datetime.now().year
    ) -> list[str]:
        logger.info(f"Initiating files path search from {start_year} to {end_year}.")

        if start_year > end_year:
            logger.error(
                f"Search failed: start_year ({start_year}) is after end_year ({end_year})."
            )
            raise ValueError(
                f"Invalid range: start_year ({start_year}) is after end_year ({end_year})."
            )

        list_files_path = []

        for year in range(start_year, end_year + 1):
            days_in_year = 366 if calendar.isleap(year) else 365
            logger.debug(f"Year {year} has {days_in_year} days.")

            for day in range(1, days_in_year + 1):
                all_files_path_day = (
                    f"{self.bucket_name}/{self.product_name}/{year}/{day:03d}/*/*.nc"
                )
                try:
                    files_path = self.fs.glob(all_files_path_day)
                    logger.info(
                        f"Year: {year}, Day: {day:03d} -> Found {len(files_path)} files."
                    )
                    list_files_path.extend(files_path)
                except Exception as e:
                    logger.warning(
                        f"Problem processing Julian day: {day}, Year: {year}. Error: {e}"
                    )

        logger.info(
            f"Yearly search complete. Total files collected: {len(list_files_path)}"
        )
        return list_files_path

    def get_files_path_by_day(
        self, start_day: int = 1, end_day: int = 1, year: int = datetime.now().year
    ) -> list[str]:
        logger.info(
            f"Initiating search for year {year}, days {start_day} to {end_day}."
        )

        if start_day > end_day:
            logger.error(
                f"Search failed: start_day ({start_day}) > end_day ({end_day})."
            )
            raise ValueError(
                f"Invalid range: start_day ({start_day}) is after end_day ({end_day})."
            )

        is_leap = calendar.isleap(year)
        max_days = 366 if is_leap else 365

        if start_day > max_days or end_day > max_days:
            logger.error(
                f"Search failed: requested day exceeds max days ({max_days}) for year {year}."
            )
            raise ValueError(f"Day 366 requested for {year}, but it's not a leap year.")

        list_files_path = []

        for day in range(start_day, end_day + 1):
            all_files_path_day = (
                f"{self.bucket_name}/{self.product_name}/{year}/{day:03d}/*/*.nc"
            )
            try:
                files_path = self.fs.glob(all_files_path_day)
                logger.info(
                    f"Processing Day {day:03d} -> Found {len(files_path)} files."
                )
                list_files_path.extend(files_path)
            except Exception as e:
                logger.warning(
                    f"Problem processing Julian day: {day}, Year: {year}. Error: {e}"
                )

        logger.info(
            f"Daily search complete. Total files collected: {len(list_files_path)}"
        )
        return list_files_path

    def get_file(self, file_path: str) -> s3fs.core.S3File:
        uri = f"s3://{file_path}"
        logger.info(f"Opening S3 stream for: {uri}")
        try:
            return self.fs.open(uri)
        except Exception as e:
            logger.error(f"Failed to open S3 file: {uri}. Error: {e}")
            raise
