# File: asf_core_data/getters/mcs/get_mcs.py
"""Getting MCS heat pump data."""

import pandas as pd
import datetime as dt
import os

from asf_core_data import PROJECT_DIR, get_yaml_config

from asf_core_data.getters.data_getters import s3, load_s3_data

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
        hps = load_s3_data(s3, BUCKET_NAME, INSTALLATIONS_RAW_S3_PATH)
    else:
        hps = pd.read_excel(local_path)

    hps = hps.astype(
        {
            "Address Line 1": str,
            "Address Line 2": str,
            "Address Line 3": str,
            "Postcode": str,
        }
    )
    hps["Commissioning Date"] = pd.to_datetime(hps["Commissioning Date"])
    hps = (
        hps.rename(columns=colnames_dict)
        .fillna({"address_1": "", "address_2": "", "address_3": ""})
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
