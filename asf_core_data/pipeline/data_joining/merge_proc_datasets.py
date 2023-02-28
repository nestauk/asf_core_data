# File: asf_core_data/pipeline/data_joining/merge_proc_datasets.py
"""Join the EPC and MCS datasets."""

# ---------------------------------------------------------------------------------

from asf_core_data.getters import data_getters
from asf_core_data.config import base_config
from asf_core_data.getters.epc import data_batches
from asf_core_data.pipeline.data_joining import merge_install_dates
from asf_core_data import load_preprocessed_epc_data
from asf_core_data.pipeline.preprocessing import data_cleaning

import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------------


def merge_proc_epc_and_mcs_installations(epc_df, verbose=False):
    """Merge processed EPC and MCS installations data.

    - Load MCS installations data (most relevant fields)
    - Mark matches in data (MCS_AVAILABLE, EPC_AVAILABLE)
    - Join based on UPRN, otherwise concatenate
    - Standardise fields such as HP_INSTALLED or HP_TYPE

    Args:
        epc_df (pd.DataFrame): Processed EPC dataframe.
        verbose (bool, optional): Print shape of dataframes as they are merged.

    Returns:
        pd.DataFrame: Merged EPC and MCS installations dataframe.
    """

    newest_joined_batch = data_batches.get_latest_mcs_epc_joined_batch()

    # # Load MCS
    mcs_df = data_getters.load_s3_data(
        base_config.BUCKET_NAME,
        newest_joined_batch,
        # TODO: Check whether we need any other fields
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

    # Get the MCS install dates (still necessary?)
    mcs_df["commission_date"] = pd.to_datetime(
        mcs_df["commission_date"]
        .str.strip()
        .str.lower()
        .replace(r"\s.*", "", regex=True)
        .replace(r"-", "", regex=True),
        format="%Y%m%d",
    )

    mcs_df.rename(columns={"postcode": "POSTCODE"}, inplace=True)

    # Tag whether MCS or EPC match is available
    mcs_df["MCS_AVAILABLE"] = True
    epc_df["EPC_AVAILABLE"] = True
    mcs_df["EPC_AVAILABLE"] = np.where(~mcs_df["UPRN"].isna(), True, False)

    mcs_matched_df = mcs_df[mcs_df["EPC_AVAILABLE"]]
    mcs_non_matched_df = mcs_df[~mcs_df["EPC_AVAILABLE"]]

    if verbose:
        print("EPC", epc_df.shape)
        print("MCS", mcs_df.shape)
        print("MCS (EPC matched)", mcs_matched_df.shape)
        print("MCS (EPC unmatched)", mcs_non_matched_df.shape)

    epc_mcs_df = pd.merge(
        epc_df,
        mcs_matched_df,
        on=["UPRN", "EPC_AVAILABLE", "MCS_AVAILABLE"],
        how="outer",
    )

    epc_mcs_df = pd.concat([epc_mcs_df, mcs_non_matched_df], axis=0)

    if verbose:
        print("EPC and MCS merged", epc_mcs_df.shape)

    # Update installation date (will only affect MCS-only records)
    epc_mcs_df["HP_INSTALL_DATE"] = np.where(
        epc_mcs_df["MCS_AVAILABLE"],
        epc_mcs_df["commission_date"],
        epc_mcs_df["HP_INSTALL_DATE"],
    )

    # Update installation tag (will only affect MCS-only records)
    epc_mcs_df["HP_INSTALLED"] = np.where(
        epc_mcs_df["MCS_AVAILABLE"], True, epc_mcs_df["HP_INSTALLED"]
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
        "No HP": "No HP",
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


def add_mcs_installer_data(epc_mcs_installations):
    """Add MCS installer data to joined EPC and MCS installations dataframe based on installer ID.

    Args:
        epc_mcs_installations (pd.DataFrame): EPC and MCS installations data.
    """

    newest_hist_inst_batch = data_batches.get_latest_hist_installers()

    print(newest_hist_inst_batch)

    # # Load MCS
    mcs_inst_data = data_getters.load_s3_data(
        base_config.BUCKET_NAME,
        newest_hist_inst_batch,
    )

    mcs_inst_data.rename(columns={"postcode": "POSTCODE"}, inplace=True)

    # TODO
    # Code to merge data with epc_mcs_installations based on company_unique_id
    # It should be an outer merge keeping, keeping installer data with no matches to MCS installations
    # Their MCS installations and EPC fields would be NaN


def merging_pipeline():
    """Merge EPC and MCS installation and installer data to create gold schema like dataset.
    - Load EPC data
    - Get best approximation for installation date
    - Merge with MCS installations and reformatting
    - Merge with MCS installers
    - Reformat postcode
    - Save output to S3

    """

    # Load the processed EPC data
    prep_epc = load_preprocessed_epc_data(
        data_path="S3", version="preprocessed", batch="newest"
    )

    # Add more precise estimations for heat pump installation dates via MCS data
    epc_with_MCS_dates = merge_install_dates.manage_hp_install_dates(prep_epc)

    # Merge EPC with MCS installations
    epc_mcs_insts = merge_proc_epc_and_mcs_installations(epc_with_MCS_dates)

    # Merge with MCS installers
    epc_mcs_complete = add_mcs_installer_data(epc_mcs_insts)

    # Reformat postcode field to include no space
    epc_mcs_complete = data_cleaning.reformat_postcode(
        epc_mcs_complete, postcode_var_name="POSTCODE", white_space="remove"
    )

    # Add geographical data

    # Save final merged dataset
    data_getters.save_to_s3(
        bucket_name="asf-core-data",
        output_var=epc_mcs_complete,
        output_file_path='"output/mcs/gold/merged_epc_mcs.csv"',
    )
