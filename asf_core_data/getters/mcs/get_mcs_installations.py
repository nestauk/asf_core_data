# File: asf_core_data/getters/mcs/get_mcs.py
"""Getting MCS heat pump data."""

import pandas as pd
import datetime as dt
import os

from asf_core_data import PROJECT_DIR, get_yaml_config

from asf_core_data.getters.data_getters import s3, load_s3_data, save_to_s3

from asf_core_data.pipeline.mcs.process.process_mcs_utils import colnames_dict

config = get_yaml_config(PROJECT_DIR / "asf_core_data/config/base.yaml")

BUCKET_NAME = config["BUCKET_NAME"]
INSTALLATIONS_RAW_S3_PATH = config["INSTALLATIONS_RAW_S3_PATH"]
INSTALLATIONS_RAW_LOCAL_PATH = config["INSTALLATIONS_RAW_LOCAL_PATH"]
RAW_DATA_S3_FOLDER = config["RAW_DATA_S3_FOLDER"]


def get_raw_installations_data(
    local_path=str(PROJECT_DIR) + INSTALLATIONS_RAW_LOCAL_PATH,
    refresh=True,
    verbose=True,
):
    """Get raw MCS HP installation data (both domestic and non-domestic)
    with no processing other than renaming columns, setting correct dtypes
    and removing duplicate records.

    Args:
        local_path (str, optional): Local path to raw data.
            Defaults to str(PROJECT_DIR) + INSTALLATIONS_RAW_LOCAL_PATH.
        refresh (bool, optional): Whether or not to update the local copy
            of the data from S3. If local_path does not exist, data will be
            pulled from S3 even if refresh is False. Defaults to True.
        verbose (bool, optional): Whether to print info about the number of
            samples before and after deduplication.

    Returns:
        DataFrame: Raw HP installation data.
    """

    if refresh or not os.path.exists(local_path):
        data = load_s3_data(s3, BUCKET_NAME, INSTALLATIONS_RAW_S3_PATH)
        hps = data.astype(
            {
                "Address Line 1": str,
                "Address Line 2": str,
                "Address Line 3": str,
                "Postcode": str,
            }
        )
        hps["Commissioning Date"] = hps["Commissioning Date"].dt.to_pydatetime()
        hps = hps.rename(columns=colnames_dict)

    else:
        hps = (
            pd.read_excel(
                local_path,
                dtype={
                    "Commissioning Date": dt.datetime,
                    "Address Line 1": str,
                    "Address Line 2": str,
                    "Address Line 3": str,
                    "Postcode": str,
                },
            )
            .rename(columns=colnames_dict)
            .convert_dtypes()
        )

    if verbose:
        print("Original # samples:", hps.shape)

    hps.fillna({"address_1": "", "address_2": "", "address_3": ""}, inplace=True)
    hps.drop_duplicates(inplace=True)

    if verbose:
        print("After dropping duplicates, # samples:", hps.shape)

    return hps
