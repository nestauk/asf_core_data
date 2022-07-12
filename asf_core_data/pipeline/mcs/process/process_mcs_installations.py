# File: asf_core_data/pipeline/mcs/process/process_mcs_installations.py
"""Processing MCS heat pump installation data."""

# %%

import pandas as pd
import re
import datetime as dt

from asf_core_data.getters.mcs_test.get_mcs_installations import (
    get_raw_installations_data,
)
from asf_core_data.pipeline.mcs.process.process_mcs_utils import clean_company_name

from asf_core_data.config import base_config

# %%


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

    return hps


#####


def get_processed_installations_data(refresh=True):
    """Get processed MCS installations data.

    Args:
        refresh (bool): Whether or not to redownload the unprocessed data
        from S3. Defaults to True.

    Returns:
        Dataframe: Processed MCS installations data.
    """

    installations_data = get_raw_installations_data(refresh=refresh)

    installations_data = add_hp_features(installations_data)
    installations_data = mask_outliers(installations_data)
    installations_data = identify_clusters(installations_data)
    # installations_data["installer_name"] = installations_data["installer_name"].apply(
    #     clean_company_name
    # )

    return installations_data
