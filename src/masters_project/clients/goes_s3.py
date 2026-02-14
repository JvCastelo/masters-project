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
        logger.info("Initiating files path search by year.")

        if start_year > end_year:
            raise ValueError(
                f"Invalid range: start_year ({start_year}) is after end_year ({end_year})."
            )

        list_files_path = []

        for year in range(start_year, end_year + 1):
            days_in_year = 366 if calendar.isleap(year) else 365
            for day in range(1, days_in_year + 1):
                logger.info(f"Processing -> Julian day: {day}, Year: {year}")
                all_files_path_day = (
                    f"{self.bucket}/{self.product}/{year}/{day:03d}/*/*.nc"
                )
                try:
                    files_path = self.fs.glob(all_files_path_day)
                    list_files_path.append(files_path)
                except Exception as e:
                    logging.info(
                        f"Problem processing Julian day: {day}, Year: {year}, Error: {e}"
                    )
        return list_files_path

    def get_files_path_by_day(
        self, start_day: int = 1, end_day: int = 1, year: int = datetime.now().year
    ) -> list[str]:
        logger.info("Initiating files path search by day.")

        if start_day > end_day:
            raise ValueError(
                f"Invalid range: start_day ({start_day}) is after end_day ({end_day})."
            )

        is_leap = calendar.isleap(year)
        max_days = 366 if is_leap else 365

        if start_day > max_days or end_day > max_days:
            raise ValueError(f"Day 366 requested for {year}, but it's not a leap year.")

        list_files_path = []

        for day in range(start_day, end_day + 1):
            logger.info(f"Processing -> Julian day: {day}, Year: {year}")
            all_files_path_day = f"{self.bucket}/{self.product}/{year}/{day:03d}/*/*.nc"
            try:
                files_path = self.fs.glob(all_files_path_day)
                list_files_path.append(files_path)
            except Exception as e:
                logging.info(
                    f"Problem processing Julian day: {day}, Year: {year}, Error: {e}"
                )
        return list_files_path

    def get_file(self, file_path: str) -> s3fs.core.S3File:
        uri = f"s3://{file_path}"
        return self.fs.open(uri)
