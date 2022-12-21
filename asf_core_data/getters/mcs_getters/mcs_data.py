# File: asf_core_data/getters/MCS/get_mcs.py
"""Getting MCS heat pump data."""


import pandas as pd
import datetime as dt
import boto3
import os

from asf_core_data.config import base_config


def get_raw_mcs_data(
    local_path=base_config.ROOT_DATA_PATH + base_config.MCS_RAW_LOCAL_PATH,
    refresh=False,
    verbose=True,
):
    """Get raw MCS HP installation data (both domestic and non-domestic)
    with no processing other than renaming columns.
    Args:
        local_path (str, optional): Local path to raw data.
            Defaults to base_config.ROOT_DATA_PATH + base_config.MCS_RAW_LOCAL_PATH.
        refresh (bool, optional): Whether or not to update the local copy
            of the data from S3. If local_path does not exist, data will be
            pulled from S3 even if refresh is False. Defaults to False.
    Returns:
        DataFrame: Raw HP installation data.
    """

    if refresh or not os.path.exists(local_path):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket("asf-core-data")
        bucket.download_file(base_config.MCS_RAW_S3_PATH, local_path)

    colnames_dict = {
        "Version Number": "version",
        "Commissioning Date": "date",
        "Address Line 1": "address_1",
        "Address Line 2": "address_2",
        "Address Line 3": "address_3",
        "Postcode": "postcode",
        "Local Authority": "local_authority",
        "Total Installed Capacity": "capacity",
        "Green Deal Installation?": "green_deal",
        "Products": "products",
        "Flow temp/SCOP ": "flow_scop",
        "Technology Type": "tech_type",
        " Installation Type": "installation_type",
        "Installation New at Commissioning Date?": "new",
        "Renewable System Design": "design",
        "Annual Space Heating Demand": "heat_demand",
        "Annual Water Heating Demand": "water_demand",
        "Annual Space Heating Supplied": "heat_supplied",
        "Annual Water Heating Supplied": "water_supplied",
        "Installation Requires Metering?": "metering",
        "RHI Metering Status": "rhi_status",
        "RHI Metering Not Ready Reason": "rhi_not_ready",
        "Number of MCS Certificates": "n_certificates",
        "Alternative Heating System Type": "alt_type",
        "Alternative Heating System Fuel Type": "alt_fuel",
        "Overall Cost": "cost",
    }

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
    hps["installation_type"] = hps["installation_type"].str.strip()
    hps.drop_duplicates(inplace=True)

    print("After preprocessing # samples:", hps.shape)

    if verbose:
        print(hps["installation_type"].value_counts(dropna=False))

    return hps


def main():
    """Main function for testing."""

    hps = get_raw_mcs_data()
    print(hps["address_3"].value_counts(dropna=False))
    print(hps.head())


if __name__ == "__main__":
    # Execute only if run as a script
    main()
