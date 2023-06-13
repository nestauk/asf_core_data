# File: asf_core_data/pipeline/mcs/process/process_mcs_installations.py
"""Processing MCS heat pump installation data."""


import pandas as pd
import re
import datetime as dt
from asf_core_data.getters.mcs_getters.get_mcs_installations import (
    get_most_recent_raw_historical_installations_data,
)
from asf_core_data.getters.mcs_getters.get_mcs_installers import (
    get_most_recent_processed_historical_installers_data,
)
from asf_core_data.config import base_config
from asf_core_data.pipeline.mcs.process.process_mcs_utils import (
    drop_instances_test_accounts,
)

# --- Legacy imports
# from asf_core_data.getters.mcs_getters.get_mcs_installations import get_raw_installations_data
# from asf_core_data.pipeline.mcs.process.process_mcs_utils import colnames_dict


def add_hp_features(hps):
    """Adds product_id, product_name, manufacturer, flow_temp, scop,
    rhi and year columns to HP installation records.
    Args:
        dhps (DataFrame): DataFrame with "products" column.
    Returns:
        DataFrame: DataFrame with additional columns.
    """

    # Extract information from product column
    product_regex_dict = {
        "product_id": "MCS Product Number: ([^|]+)",
        "product_name": "Product Name: ([^|]+)",
        "manufacturer": "License Holder: ([^|]+)",
        "flow_temp": "Flow Temp: ([^|]+)",
        "scop": "SCOP: ([^)]+)",
    }
    for product_feat, regex in product_regex_dict.items():
        hps[product_feat] = [
            re.search(regex, product).group(1).strip() for product in hps["products"]
        ]

    # Add RHI field - any "Unspecified" values in rhi_status field signify
    # that the installation is not for DRHI, missing values are unknown
    if "rhi_status" in hps.columns:
        hps["rhi"] = True
        hps.loc[(hps["rhi_status"] == "Unspecified"), "rhi"] = False
        hps["rhi"].mask(hps["rhi_status"].isna())

    # Add installation year
    hps["commission_year"] = hps["commission_date"].dt.year

    return hps


def mask_outliers(hps, max_cost=base_config.MCS_MAX_COST):
    """Replace 'unreasonable' values in HP installation data with NA.
    'Unreasonable' values are cost values that are 0 or above the max_cost,
    flow_temp values less than or equal to 0, and scop values that are 0.
    Args:
        dhps (DataFrame): DataFrame with cost, flow_temp and scop columns.
        max_cost (numeric): Maximum allowed cost value in £.
    Returns:
        DataFrame: HP installations with masked outliers.
    """
    hps["cost"] = hps["cost"].mask((hps["cost"] <= 0) | (hps["cost"] > max_cost))

    hps["flow_temp"] = pd.to_numeric(hps["flow_temp"])
    hps["flow_temp"] = hps["flow_temp"].mask(hps["flow_temp"] <= 0)

    hps["scop"] = pd.to_numeric(hps["scop"].mask(hps["scop"] == "Unspecified"))
    hps["scop"] = hps["scop"].mask(hps["scop"] == 0)

    hps["capacity"] = hps["capacity"].mask(hps["capacity"] < 0)

    return hps


def fill_missing_installation_type(installations: pd.DataFrame) -> pd.DataFrame:
    """
    Fills missing installation_type with end_user_installation_type.

    Args:
        installations: MCS installations data
    Return:
        Installations data with updated installation type.
    """
    installations["installation_type"] = installations["installation_type"].fillna(
        installations["end_user_installation_type"]
    )

    installations.drop("end_user_installation_type", inplace=True)

    return installations


def identify_clusters(hps, time_interval=base_config.MCS_CLUSTER_TIME_INTERVAL):
    """Label HP records with whether they form part of a 'cluster'
    of installations in the same postcode and around the same time.
    This suggests that these installations were done en masse.
    Args:
        dhps (DataFrame): DataFrame of HP installations with 'postcode' and 'commission_date' columns.
        time_interval (int, optional): Maximum gap between two installations in the same postcode.
        Defaults to base_config.MCS_CLUSTER_TIME_INTERVAL.
    Returns:
        DataFrame: DataFrame with added 'cluster' column.
    """

    # Compute difference between consecutive installations within each postcode
    # (both backwards and forwards - otherwise we'd miss the first/last in each cluster)
    hps["diff_bwd"] = (
        hps.sort_values(["postcode", "commission_date"])
        .groupby("postcode")["commission_date"]
        .diff()
    )
    hps["diff_fwd"] = (
        hps.sort_values(["postcode", "commission_date"])
        .groupby("postcode")["commission_date"]
        .diff(periods=-1)
    )

    # Flag as cluster if within time_interval days of another installation
    hps["cluster"] = False
    hps["cluster"].loc[
        (hps["diff_bwd"] <= dt.timedelta(days=time_interval))
        | (hps["diff_fwd"] >= dt.timedelta(days=-time_interval))
    ] = True

    hps.drop(columns=["diff_bwd", "diff_fwd"], inplace=True)

    return hps


def get_installer_unique_id(
    installations: pd.DataFrame, installers: pd.DataFrame
) -> pd.DataFrame:
    """
    Updates installations table by adding the unique installer ID.
    Args:
        installations: installations table
        installers: historical installers table
    """

    installations = installations.merge(
        right=installers[["company_name", "company_unique_id"]].drop_duplicates(
            "company_name"
        ),
        how="left",
        left_on="installer_name",
        right_on="company_name",
    )

    installations.drop(columns=["company_name"], inplace=True)

    return installations


def get_processed_installations_data():
    """Process MCS installations data and add information about company unique ID.

    Returns:
        Dataframe: Processed MCS installations data.
    """

    # installations_data = get_raw_installations_data(refresh=refresh) -> legacy function
    installations_data = get_most_recent_raw_historical_installations_data()

    installations_data = installations_data.rename(
        columns=base_config.historical_installations_rename_cols_dict
    )

    installations_data = drop_instances_test_accounts(
        installations_data, "installer_name"
    )

    installations_data.drop_duplicates(inplace=True)

    installations_data = add_hp_features(installations_data)
    installations_data = mask_outliers(installations_data)
    installations_data = fill_missing_installation_type(installations_data)
    installations_data = identify_clusters(installations_data)

    # getting latest batch of processed historical installers data
    historical_installers_processed_data = (
        get_most_recent_processed_historical_installers_data()
    )

    # Adding variable with unique installer ID
    installations_data = get_installer_unique_id(
        installations_data, historical_installers_processed_data
    )

    return installations_data
