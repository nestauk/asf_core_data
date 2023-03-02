"""
Functions to get MCS certified heat pump installers data from S3 in a usable format with correct dtypes.

"""

import pandas as pd
import os
import boto3
from asf_core_data.getters.data_getters import load_s3_data, get_most_recent_batch_name
from asf_core_data.config import base_config


def get_most_recent_raw_historical_installers_data() -> pd.DataFrame:
    """
    Get the most recent version of the raw historical heat pump MCS certified installer data from S3.

    Returns:
        The most recent version of the raw historical installers data from MCS.
    """

    bucket = base_config.BUCKET_NAME
    path = base_config.MCS_HISTORICAL_DATA_INPUTS_PATH
    most_recent_file_name = get_most_recent_batch_name(
        bucket=bucket, s3_folder_path=path, filter_keep_keywords=["installer"]
    )

    return load_s3_data(
        bucket_name=base_config.BUCKET_NAME,
        file_name=most_recent_file_name,
        dtypes=base_config.raw_historical_installers_dtypes,
    )


def get_raw_historical_installers_data(
    raw_historical_installers_file_name: str,
) -> pd.DataFrame:
    """
    Get a specified version of the raw historical heat pump MCS certified installer data from S3.

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


def get_most_recent_processed_historical_installers_data() -> pd.DataFrame:
    """
    Get the most recent version of the processed historical heat pump MCS certified installer data from S3.
     Returns:
        The most recent version of the processed historical installers data from MCS.
    """
    bucket = base_config.BUCKET_NAME
    path = base_config.MCS_HISTORICAL_DATA_OUTPUTS_PATH
    most_recent_file_name = get_most_recent_batch_name(
        bucket=bucket, s3_folder_path=path
    )

    return load_s3_data(
        bucket_name=bucket,
        file_name=most_recent_file_name,
        dtypes=base_config.preprocessed_historical_installers_dtypes,
        columns_to_parse_as_dates=base_config.preprocessed_historical_installers_date_cols,
    )


def get_processed_historical_installers_data(path_to_file: str) -> pd.DataFrame:
    """
    Get a specified version of the processed historical heat pump MCS certified installer data from S3.

    Args:
        path_to_file: string of format 'outputs/MCS/installers/mcs_historical_installers_YYYYMMDD.csv',
        where YYYY/MM/DD is the date MCS shared the file in the Data Dumps Google Drive folder.
    Returns:
        Raw historical installers data from MCS.
    """
    return load_s3_data(
        bucket_name=base_config.BUCKET_NAME,
        file_name=path_to_file,
        dtypes=base_config.preprocessed_historical_installers_dtypes,
        columns_to_parse_as_dates=base_config.preprocessed_historical_installers_date_cols,
    )
