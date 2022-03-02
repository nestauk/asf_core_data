# File: asf_core_data/pipeline/process_mcs.py
"""Processing MCS heat pump data."""

import pandas as pd
import re
import datetime as dt

from asf_core_data import PROJECT_DIR, get_yaml_config, Path
from asf_core_data.getters.mcs.get_mcs import get_raw_mcs_data

config = get_yaml_config(Path(str(PROJECT_DIR) + "/asf_core_data/config/base.yaml"))

MCS_PROCESSED_PATH = config["MCS_PROCESSED_PATH"]
MAX_COST = config["MCS_MAX_COST"]
# MAX_CAPACITY = config["MCS_MAX_CAPACITY"]
CLUSTER_TIME_INTERVAL = config["MCS_CLUSTER_TIME_INTERVAL"]


def remove_old_records(hps):
    """Remove records that are duplicated or have a newer version.
    Removes duplicates, groups by address, then only takes the
    record with highest version.

    Args:
        hps (Dataframe): HP installation records.

    Returns:
        Dataframe: HP installation records without old records.
    """
    most_recent_indices = hps.groupby(["address_1", "address_2", "address_3"])[
        "version"
    ].idxmax()

    return hps.iloc[most_recent_indices].reset_index(drop=True)


def add_columns(hps):
    """Adds product_id, product_name, manufacturer, flow_temp, scop,
    rhi and year columns to HP installation records.

    Args:
        dhps (DataFrame): DataFrame with "products" column.

    Returns:
        DataFrame: DataFrame with additional columns.
    """

    # Extract information from product column
    product_regex_dict = {
        "product_id": "MCS Product Number: ([^\|]+)",
        "product_name": "Product Name: ([^\|]+)",
        "manufacturer": "License Holder: ([^\|]+)",
        "flow_temp": "Flow Temp: ([^\|]+)",
        "scop": "SCOP: ([^\)]+)",
    }
    for product_feat, regex in product_regex_dict.items():
        hps[product_feat] = [
            re.search(regex, product).group(1).strip() for product in hps["products"]
        ]

    # Add RHI field - any "Unspecified" values in rhi_status field signify
    # that the installation is not for DRHI, missing values are unknown
    hps["rhi"] = True
    hps.loc[(hps["rhi_status"] == "Unspecified"), "rhi"] = False
    hps["rhi"].mask(hps["rhi_status"].isna())

    # Add installation year
    hps["year"] = hps["date"].dt.year

    return hps


def mask_outliers(hps, max_cost=MAX_COST):
    """Replace unreasonable values in HP installation data with NA.

    Args:
        dhps (DataFrame): DataFrame with cost, flow_temp and scop columns.
        max_cost (numeric): Maximum allowed cost value in £.

    Returns:
        [type]: [description]
    """
    hps["cost"] = hps["cost"].mask((hps["cost"] == 0) | (hps["cost"] > max_cost))

    hps["flow_temp"] = pd.to_numeric(hps["flow_temp"])
    hps["flow_temp"] = hps["flow_temp"].mask(hps["flow_temp"] <= 0)

    hps["scop"] = pd.to_numeric(hps["scop"].mask(hps["scop"] == "Unspecified"))
    hps["scop"] = hps["scop"].mask(hps["scop"] == 0)

    return hps


def identify_clusters(hps, time_interval=CLUSTER_TIME_INTERVAL):
    """Label HP records with whether they form part of a "cluster"
    of installations in the same postcode and around the same time.
    This suggests that these installations were done en masse.

    Args:
        dhps (DataFrame): DataFrame of HP installations with "postcode" and "date" columns.
        time_interval (int, optional): Maximum gap between two installations in the same postcode.
        Defaults to CLUSTER_TIME_INTERVAL.

    Returns:
        DataFrame: DataFrame with added "cluster" column.
    """

    hps["diff_bwd"] = (
        hps.sort_values(["postcode", "date"]).groupby("postcode")["date"].diff()
    )
    hps["diff_fwd"] = (
        hps.sort_values(["postcode", "date"])
        .groupby("postcode")["date"]
        .diff(periods=-1)
    )

    hps["cluster"] = False
    hps["cluster"].loc[
        (hps["diff_bwd"] <= dt.timedelta(days=time_interval))
        | (hps["diff_fwd"] >= dt.timedelta(days=-time_interval))
    ] = True

    return hps


#####


def get_processed_mcs_data(save=True):

    data = get_raw_mcs_data()

    data = remove_old_records(data)
    data = add_columns(data)
    data = mask_outliers(data)
    data = identify_clusters(data)

    if save:
        data.to_csv(str(PROJECT_DIR) + MCS_PROCESSED_PATH)

    return data
