import logging

import pandas as pd

logger = logging.getLogger(__name__)


class DatasetMerger:
    """Merge GOES and SONDA DataFrames on timestamp columns (inner join, timezone-aware)."""

    @staticmethod
    def merge_goes_sonda(
        df_goes: pd.DataFrame,
        df_sonda: pd.DataFrame,
        time_col_goes: str = "timestamp",
        time_col_sonda: str = "timestamp",
    ) -> pd.DataFrame:
        """Inner-join GOES and SONDA on timestamp; ensure UTC datetimes.

        Args:
            df_goes: GOES satellite feature DataFrame.
            df_sonda: SONDA ground measurement DataFrame.
            time_col_goes: Name of the timestamp column in df_goes. Defaults to "timestamp".
            time_col_sonda: Name of the timestamp column in df_sonda. Defaults to "timestamp".

        Returns:
            Merged DataFrame with rows only where timestamps match.

        Raises:
            Exception: If timestamp parsing fails (re-raised after logging).
            ValueError: If the inner merge produces zero rows.
        """
        logger.info("Starting exact GOES and SONDA dataset merge.")

        try:
            logger.debug("Casting timestamp columns to timezone-aware UTC datetimes...")
            df_goes[time_col_goes] = pd.to_datetime(df_goes[time_col_goes], utc=True)
            df_sonda[time_col_sonda] = pd.to_datetime(
                df_sonda[time_col_sonda], utc=True
            )
        except Exception:
            logger.exception("Failed to parse timestamps. Check your CSV date formats.")
            raise

        df_merged = pd.merge(
            left=df_goes,
            right=df_sonda,
            left_on=time_col_goes,
            right_on=time_col_sonda,
            how="inner",
        )

        if df_merged.empty:
            logger.error(
                "CRITICAL: The inner merge resulted in 0 rows! "
                "GOES and SONDA timestamps did not align. Check your timezone formatting."
            )
            raise ValueError(
                "Merged dataset is empty. Pipeline halted to prevent data loss."
            )

        logger.info(
            f"Merge successful. Final shape: {df_merged.shape[0]} rows, {df_merged.shape[1]} columns. "
            f"(Dropped {len(df_goes) - len(df_merged)} unmatched GOES scans)."
        )

        return df_merged
