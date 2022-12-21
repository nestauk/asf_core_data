# File: asf_core_data/getters/mcs/get_mcs_installations.py
"""Function to get concatenated installations data in a usable format,
with correct dtypes."""

# %%

import pandas as pd
import os

from asf_core_data.getters.data_getters import load_s3_data

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
