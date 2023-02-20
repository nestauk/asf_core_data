"""
Functions to get MCS certified heat pump installers data from S3 in a usable format with correct dtypes.

"""

import pandas as pd
import os

from asf_core_data.getters.data_getters import load_s3_data

from asf_core_data.config import base_config


def get_raw_historical_mcs_installers(
    raw_historical_installers_file_name: str,
) -> pd.DataFrame:
    """
    Get raw historical heat pump MCS certified installer data from S3.

    Args:
        raw_historical_mcs_installers_file_name: name of file in S3 bucket, `raw_historical_installers_YYYYMMDD.xlsx`
        where YYYY/MM/DD is the date MCS shared the file in the Data Dumps Google Drive folder.
    Returns:
        Raw historical installers heat pump data from MCS.
    """
    return load_s3_data(
        bucket_name=base_config.BUCKET_NAME,
        file_name=os.path.join(
            base_config.MCS_HISTORICAL_DATA_INPUTS_PATH,
            raw_historical_installers_file_name,
        ),
        dtypes=base_config.raw_historical_installers_dtypes,
    )


def get_processed_historical_installers_data(date: str) -> pd.DataFrame:
    """
    Get raw historical heat pump MCS certified installer data from S3.

    Args:
        date: YYYYMMDD, wwhere YYYY/MM/DD is the date MCS shared the file in the Data Dumps Google Drive folder.
    Returns:
        Raw historical installers heat pump data from MCS.
    """
    return load_s3_data(
        bucket_name=base_config.BUCKET_NAME,
        file_name=base_config.PREPROCESSED_MCS_HISTORICAL_INSTALLERS_FILE_PATH.format(
            date
        )[1:],
        dtypes=base_config.preprocessed_historical_installers_dtypes,
        columns_to_parse_as_dates=base_config.preprocessed_historical_installers_date_cols,
    )
