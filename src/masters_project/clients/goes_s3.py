import calendar
import logging
from datetime import datetime

import s3fs

from masters_project.enums import GoesChannelEnums
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
    def _fetch_paths(
        self, year: int, start_day: int, end_day: int, channels: list[str] = None
    ) -> list[str]:

        if not channels:
            channels = [channel_enum.value for channel_enum in GoesChannelEnums]

        paths = []

        for channel in channels:
            for day in range(start_day, end_day + 1):
                pattern = f"{self.bucket_name}/{self.product_name}/{year}/{day:03d}/*/*{channel}*.nc"

                try:
                    found = self.fs.glob(pattern)
                    logger.info(
                        f"Year {year}, Day {day:03d}, Channel {channel} -> Found {len(found)} files."
                    )
                    paths.extend(found)

                except Exception as e:
                    logger.error(
                        f"Error on Day {day:03d}, Year {year}, Channel {channel}: {e}"
                    )

        return paths

    @measure_memory
    @time_track
    def get_files_path_by_year(
        self, start_year: int = 2017, end_year: int = None, channels: list[str] = None
    ) -> list[str]:
        end_year = end_year or datetime.now().year

        logger.info(f"Initiating files path search from {start_year} to {end_year}.")

        if start_year > end_year:
            logger.error(
                f"Search failed: start_year ({start_year}) is after end_year ({end_year})."
            )
            raise ValueError(
                f"Invalid range: start_year ({start_year}) is after end_year ({end_year})."
            )

        all_paths = []

        for year in range(start_year, end_year + 1):
            max_days = 366 if calendar.isleap(year) else 365

            all_paths.extend(self._fetch_paths(year, 1, max_days, channels))

        logger.info(f"Yearly search complete. Total files collected: {len(all_paths)}")

        return all_paths

    @measure_memory
    @time_track
    def get_files_path_by_day(
        self,
        start_day: int = 1,
        end_day: int = 1,
        year: int = None,
        channels: list[str] = None,
    ) -> list[str]:
        year = year or datetime.now().year

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

        all_paths = self._fetch_paths(year, start_day, end_day, channels)

        logger.info(f"Daily search complete. Total files collected: {len(all_paths)}")

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
