import logging

import pandas as pd

logger = logging.getLogger(__name__)


class DatasetMerger:
    @staticmethod
    def merge_goes_sonda(
        df_goes: pd.DataFrame,
        df_sonda: pd.DataFrame,
        time_col_goes: str = "timestamp",
        time_col_sonda: str = "timestamp",
    ) -> pd.DataFrame:

        logger.info("Starting exact GOES and SONDA dataset merge.")

        df_merged = pd.merge(
            left=df_goes,
            right=df_sonda,
            left_on=time_col_goes,
            right_on=time_col_sonda,
            how="inner",
        )

        logger.info(
            f"Merge complete. Resulting model-ready dataset has {len(df_merged)} rows. "
            f"(Dropped {len(df_goes) - len(df_merged)} unmatched GOES scans)."
        )

        return df_merged
