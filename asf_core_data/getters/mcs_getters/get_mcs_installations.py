# File: asf_core_data/getters/mcs/get_mcs_installations.py
"""Function to get concatenated installations data in a usable format,
with correct dtypes."""

# %%

import pandas as pd
import os

from asf_core_data.getters.data_getters import load_s3_data, get_most_recent_batch_name

from asf_core_data.config import base_config

# %%


def get_raw_installations_data(
    local_path=base_config.ROOT_DATA_PATH + base_config.INSTALLATIONS_RAW_LOCAL_PATH,
    refresh=True,
    verbose=True,
):
    """Get raw MCS HP installation data (both domestic and non-domestic)
    with no processing other than renaming columns and setting correct dtypes.

    Args:
        local_path (str, optional): Local path to raw data.
            Defaults to str(PROJECT_DIR) + INSTALLATIONS_RAW_LOCAL_PATH.
        refresh (bool, optional): Whether or not to update the local copy
            of the data from S3. If local_path does not exist, data will be
            pulled from S3 even if refresh is False. Defaults to True.
        verbose (bool, optional): Whether to print info about the number of
            samples in loaded data.

    Returns:
        DataFrame: Raw HP installation data.
    """

    if refresh or not os.path.exists(local_path):
        hps = load_s3_data(
            base_config.BUCKET_NAME, base_config.INSTALLATIONS_RAW_S3_PATH, usecols=None
        )
    else:
        hps = pd.read_excel(local_path)

    hps = hps.astype(
        {
            "address_1": str,
            "address_2": str,
            "address_3": str,
            "postcode": str,
        }
    )
    hps["commission_date"] = pd.to_datetime(hps["commission_date"])
    hps = (
        hps.convert_dtypes()
        .drop_duplicates(
            subset=[
                "version",
                "commission_date",
                "address_1",
                "address_2",
                "address_3",
                "postcode",
            ]
        )
        .reset_index(drop=True)
    )

    if verbose:
        print("Shape of loaded data:", hps.shape)

    return hps


def get_raw_mcs_installations_concatenated_files():
    """
    Get raw MCS installations concatenated files.
    """
    return load_s3_data(
        bucket_name=base_config.BUCKET_NAME,
        file_name="inputs/MCS/mcs_installations.csv",
        columns_to_parse_as_dates=["commission_date"],
    )


def get_most_recent_raw_historical_installations_data() -> pd.DataFrame:
    """
    Get the most recent version of the raw historical heat pump MCS certified installation data from S3.

    Returns:
        The most recent version of the raw historical installations data from MCS.
    """

    bucket = base_config.BUCKET_NAME
    path = base_config.MCS_HISTORICAL_DATA_INPUTS_PATH
    most_recent_file_name = get_most_recent_batch_name(
        bucket=bucket, s3_folder_path=path, filter_keep_keywords=["installation"]
    )

    return load_s3_data(
        bucket_name=base_config.BUCKET_NAME,
        file_name=most_recent_file_name,
        dtypes=base_config.raw_historical_installations_dtypes,
    )


def get_raw_historical_installations_data(
    raw_historical_installations_file_name: str,
) -> pd.DataFrame:
    """
    Get a specified version of the raw MCS historical installation data (both domestic and non-domestic)
    for S3 bucket.

    Args:
        raw_historical_mcs_installations_file_name: name of file in S3 bucket, raw_historical_installations_YYYYMMDD.xlsx
        where YYYY/MM/DD is the date MCS shared the file in the Data Dumps Google Drive folder.
    Returns:
        Raw historical installation data
    """
    return load_s3_data(
        bucket_name=base_config.BUCKET_NAME,
        file_name=os.path.join(
            base_config.MCS_HISTORICAL_DATA_INPUTS_PATH,
            raw_historical_installations_file_name,
        ),
        dtypes=base_config.raw_historical_installations_dtypes,
    )
