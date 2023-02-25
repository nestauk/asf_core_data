# ---------------------------------------------------------------------------------

from asf_core_data.getters import data_getters
from asf_core_data.config import base_config
from asf_core_data.getters.epc import data_batches

import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------------


def merge_proc_epc_and_mcs_installs(epc_df):
    """Merge processed EPC and MCS installations data

    Args:
        epc_df (pd.DataFrame): Processed EPC dataframe.

    Returns:
        pd.DataFrame: Merged EPC and MCS installations dataframe.
    """

    newest_joined_batch = data_batches.get_latest_mcs_epc_joined_batch()

    # # Load MCS
    mcs_df = data_getters.load_s3_data(
        base_config.BUCKET_NAME,
        newest_joined_batch,
        usecols=[
            "UPRN",
            "commission_date",
            "capacity",
            "estimated_annual_generation",
            "flow_temp",
            "tech_type",
            "scop",
            "design",
            "product_name",
            "manufacturer",
            "cost",
        ],
        dtype={"UPRN": "str", "commission_date": "str"},
    )

    # Get the MCS install dates
    mcs_df["commission_date"] = pd.to_datetime(
        mcs_df["commission_date"]
        .str.strip()
        .str.lower()
        .replace(r"\s.*", "", regex=True)
        .replace(r"-", "", regex=True),
        format="%Y%m%d",
    )

    mcs_df["MCS_AVAILABLE"] = True
    epc_df["EPC_AVAILABLE"] = True
    mcs_df["EPC_AVAILABLE"] = np.where(~mcs_df["UPRN"].isna(), True, False)

    mcs_matched_df = mcs_df[mcs_df["EPC_AVAILABLE"]]
    mcs_non_matched_df = mcs_df[~mcs_df["EPC_AVAILABLE"]]

    # print(mcs_df["EPC_AVAILABLE"].value_counts(dropna=False))
    # print(epc_df["MCS_AVAILABLE"].value_counts(dropna=False))
    # print(mcs_df.shape)
    # print(epc_df.shape)

    epc_mcs_df = pd.merge(
        epc_df,
        mcs_matched_df,
        on=["UPRN", "EPC_AVAILABLE", "MCS_AVAILABLE"],
        how="outer",
    )

    epc_mcs_df = pd.concat([epc_mcs_df, mcs_non_matched_df], axis=0)

    epc_mcs_df["HP_INSTALL_DATE"] = np.where(
        epc_mcs_df["MCS_AVAILABLE"],
        epc_mcs_df["commission_date"],
        epc_mcs_df["HP_INSTALL_DATE"],
    )

    epc_mcs_df["HP_INSTALLED"] = np.where(
        (epc_mcs_df["HP_INSTALLED"]) | (epc_mcs_df["MCS_AVAILABLE"]), True, False
    )

    epc_mcs_df = standardise_hp_type(epc_mcs_df)

    return epc_mcs_df


def standardise_hp_type(epc_mcs_df):
    """Standardise HP type between EPC and MCS data.

    Args:
        epc_mcs_df (pd.DataFrame): Merged EPC and MCS dataframe.

    Returns:
        pd.DataFrame: Merged EPC and MCS dataframe with standardised HP type.
    """

    type_dict = {
        "No HP": np.NaN,
        "air source heat pump": "Air Source Heat Pump",
        "ground source heat pump": "Ground/Water Source Heat Pump",
        "heat pump": "Undefined or Other Heat Pump Type",
        "water source heat pump": "Ground/Water Source Heat Pump",
        "community heat pump": "Undefined or Other Heat Pump Type",
        "Exhaust Air Heat Pump": "Undefined or Other Heat Pump Type",
        "Air Source Heat Pump": "Air Source Heat Pump",
        "Ground/Water Source Heat Pump": "Ground/Water Source Heat Pump",
    }

    # Overwrite with tech type from MCS if MCS data exists
    epc_mcs_df["HP_TYPE"] = np.where(
        epc_mcs_df["MCS_AVAILABLE"], epc_mcs_df["tech_type"], epc_mcs_df["HP_TYPE"]
    )

    epc_mcs_df["HP_TYPE"] = epc_mcs_df["HP_TYPE"].map(type_dict)

    return epc_mcs_df
