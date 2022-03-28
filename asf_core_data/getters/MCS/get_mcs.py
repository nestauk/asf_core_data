# File: asf_core_data/getters/MCS/get_mcs.py
"""Getting MCS heat pump data."""

import pandas as pd
import datetime as dt
import boto3
import os

from asf_core_data import PROJECT_DIR, get_yaml_config, Path, bucket_name

from asf_core_data.getters.data_getters import s3, load_s3_data

from asf_core_data.pipeline.mcs.process.process_mcs_utils import colnames_dict

config = get_yaml_config(Path(str(PROJECT_DIR) + "/asf_core_data/config/base.yaml"))

BUCKET_NAME = config["BUCKET_NAME"]
MCS_RAW_S3_PATH = config["MCS_RAW_S3_PATH"]
MCS_RAW_LOCAL_PATH = config["MCS_RAW_LOCAL_PATH"]

def get_raw_mcs_data(
    local_path=str(PROJECT_DIR) + MCS_RAW_LOCAL_PATH, refresh=False, verbose=True
):
    """Get raw MCS HP installation data (both domestic and non-domestic)
    with no processing other than renaming columns.

    Args:
        local_path (str, optional): Local path to raw data.
            Defaults to str(PROJECT_DIR)+MCS_RAW_LOCAL_PATH.
        refresh (bool, optional): Whether or not to update the local copy
            of the data from S3. If local_path does not exist, data will be
            pulled from S3 even if refresh is False. Defaults to False.

    Returns:
        DataFrame: Raw HP installation data.
    """

    if refresh or not os.path.exists(local_path):
        data = load_s3_data(s3, bucket_name, MCS_RAW_S3_PATH)
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
    # hps["installation_type"] = hps["installation_type"].str.strip()
    hps.drop_duplicates(inplace=True)

    print("After preprocessing # samples:", hps.shape)

    # if verbose:
    #     print(hps["installation_type"].value_counts(dropna=False))

    return hps


def main():
    """Main function for testing."""

    hps = get_raw_mcs_data()
    print(hps["address_3"].value_counts(dropna=False))
    print(hps.head())


if __name__ == "__main__":
    # Execute only if run as a script
    main()
