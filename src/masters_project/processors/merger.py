import logging
from functools import reduce

import pandas as pd

logger = logging.getLogger(__name__)


class DatasetMerger:
    """General utility to merge and concat GOES and SONDA dataframes."""

    @staticmethod
    def _prepare_dataframes(
        dataframes: list[pd.DataFrame], time_col: str
    ) -> list[pd.DataFrame]:
        """Ensures all DFs have UTC-aware timestamps and no missing time values."""
        prepared = []
        for i, df in enumerate(dataframes):
            df_copy = df.copy()
            try:
                df_copy[time_col] = pd.to_datetime(df_copy[time_col], utc=True)
                prepared.append(df_copy)
            except Exception as e:
                logger.error(f"Failed to parse time_col in dataframe at index {i}: {e}")
                raise
        return prepared

    @classmethod
    def join_by_columns(
        cls,
        dataframes: list[pd.DataFrame],
        time_col: str = "timestamp",
        how: str = "inner",
    ) -> pd.DataFrame:
        """
        Merge multiple DFs side-by-side on a common timestamp.
        Use this to combine different sensors (e.g., Satellite + Ground Station).
        """
        logger.info(f"Joining {len(dataframes)} datasets on '{time_col}' ({how} join).")
        prepared = cls._prepare_dataframes(dataframes, time_col)

        df_merged = reduce(
            lambda left, right: pd.merge(left, right, on=time_col, how=how), prepared
        )

        if df_merged.empty:
            logger.warning("Join result is empty! Timestamps may not align.")

        return df_merged

    @classmethod
    def concat_by_rows(
        cls, dataframes: list[pd.DataFrame], time_col: str = "timestamp"
    ) -> pd.DataFrame:
        """
        Stack multiple DFs vertically.
        Use this to append new days/months of the same sensor data.
        """
        logger.info(f"Stacking {len(dataframes)} datasets vertically.")
        prepared = cls._prepare_dataframes(dataframes, time_col)

        df_stacked = pd.concat(prepared, axis=0, ignore_index=True)

        return df_stacked.sort_values(time_col).reset_index(drop=True)
