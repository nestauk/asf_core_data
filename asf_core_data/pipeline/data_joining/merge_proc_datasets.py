# File: asf_core_data/pipeline/data_joining/merge_proc_datasets.py
"""Join the EPC and MCS installations and installer datasets.
These scripts are meant to merge the final processed datasets into a gold dataset.

The output is a complete dataframe with all EPC records (dedupl) and MCS installations and installers.
We use outer merges to avoid losing data, creating NaN values for missing records.

    - Load EPC data
    - Get best approximation for installation date
    - Merge with MCS installations and reformatting
    - Merge with MCS installers
    - Reformat postcode and geographies
    - Save output to S3
"""

# ---------------------------------------------------------------------------------

from asf_core_data.getters import data_getters
from asf_core_data.config import base_config
from asf_core_data.getters.epc import data_batches
from asf_core_data.pipeline.data_joining import install_date_computation
from asf_core_data import load_preprocessed_epc_data
from argparse import ArgumentParser
from datetime import date
import pandas as pd
import numpy as np
from datetime import datetime
from asf_core_data.pipeline.preprocessing import feature_engineering
from asf_core_data.getters import data_getters

# ---------------------------------------------------------------------------------


def add_mcs_installations_data(
    epc_df,
    usecols=base_config.MCS_INSTALLATIONS_FEAT_SELECTION_MERGED_DATASET,
    bucket_name=base_config.BUCKET_NAME,
    verbose=False,
):
    """Add MCS installations data to EPC data.

    - Load MCS installations data (most relevant fields)
    - Mark matches in data (MCS_AVAILABLE, EPC_AVAILABLE)
    - Join based on UPRN, otherwise concatenate
    - Standardise fields such as HP_INSTALLED or HP_TYPE

    Args:
        epc_df (pd.DataFrame): Processed EPC dataframe.
        usecols (list, optional): MCS features to use. Defaults to base_config.BASIC_MCS_FIELDS.
        bucket_name (str, optional): Bucket name (from where to load from). Defaults to base_config.BUCKET_NAME.
        verbose (bool, optional): Print shape of dataframes as they are merged (defaults to False).

    Returns:
        pd.DataFrame: Merged EPC and MCS installations dataframe.
    """

    # We get the latest MCS-EPC joined dataset because we need the UPRN from EPC
    # but the only remaining columns we get are MCS columns
    newest_joined_batch = data_batches.get_latest_mcs_epc_joined_batch()

    # # Load MCS
    mcs_df = data_getters.load_s3_data(
        bucket_name,
        newest_joined_batch,
        usecols=usecols,
        dtype={"UPRN": "str", "commission_date": "str"},
    )

    mcs_df = install_date_computation.reformat_mcs_date(mcs_df, "commission_date")
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

    # we already have the postcode from EPC for matched records
    mcs_matched_df.drop(columns="POSTCODE", inplace=True)

    epc_mcs_df = pd.merge(
        epc_df,
        mcs_matched_df,
        on=["UPRN", "EPC_AVAILABLE", "MCS_AVAILABLE"],
        how="outer",  # same as a left merge in this case
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
    epc_mcs_df.drop(columns="commission_date", inplace=True)

    # Update installation tag (will only affect MCS-only records)
    epc_mcs_df["HP_INSTALLED"] = np.where(
        epc_mcs_df["MCS_AVAILABLE"], True, epc_mcs_df["HP_INSTALLED"]
    )

    # Update TECH_TYPE and standardise HP types (will only affect MCS-only records)
    epc_mcs_df = standardise_hp_type(epc_mcs_df)

    # Update HEATING_SYSTEM (will only affect MCS-only records)
    epc_mcs_df["HEATING_SYSTEM"] = np.where(
        epc_mcs_df["MCS_AVAILABLE"], "heat pump", epc_mcs_df["HEATING_SYSTEM"]
    )

    # Update HEATING_FUEL (will only affect MCS-only records)
    epc_mcs_df["HEATING_FUEL"] = np.where(
        epc_mcs_df["MCS_AVAILABLE"], "electric", epc_mcs_df["HEATING_FUEL"]
    )

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
    epc_mcs_df.drop(columns="tech_type", inplace=True)

    epc_mcs_df["HP_TYPE"] = epc_mcs_df["HP_TYPE"].map(type_dict)

    return epc_mcs_df


def add_mcs_installer_data(
    df, usecols=base_config.MCS_INSTALLER_FEAT_SELECTION_MERGED_DATASET
):
    """Add MCS installer data to given dataframe based on installer ID.
    The dataframe can be EPC and MCS installations data combined, or simply MCS installations data.

    The MCS installations data needs to include the following fields: "company_unique_id", "installer_name".

    Args:
        df (pd.DataFrame): EPC and MCS installations data.
    """

    # Add fields required for merging
    if usecols is not None:
        usecols = list(set(usecols + ["company_unique_id", "company_name"]))

    newest_hist_inst_batch = data_batches.get_latest_hist_installers()

    # Load MCS
    mcs_instllr_data = data_getters.load_s3_data(
        base_config.BUCKET_NAME, newest_hist_inst_batch, usecols=usecols
    )

    mcs_instllr_data.rename(columns={"postcode": "POSTCODE"}, inplace=True)

    merged_df = df.merge(
        right=mcs_instllr_data,
        how="outer",
        left_on=["company_unique_id", "installer_name"],
        right_on=["company_unique_id", "company_name"],
    )

    return merged_df


def merging_pipeline(
    epc_usecols=base_config.EPC_FEAT_SELECTION_MERGED_DATASET,
    mcs_installations_usecols=base_config.MCS_INSTALLATIONS_FEAT_SELECTION_MERGED_DATASET,
    mcs_installers_usecols=base_config.MCS_INSTALLER_FEAT_SELECTION_MERGED_DATASET,
    path_to_data="S3",
    verbose=False,
):
    """Merge EPC and MCS installation and installer data to create a complete MCS/EPC dataset.

    The output is a complete dataframe with all EPC records (dedupl) and MCS installations and installers.
    We use outer merges to avoid losing data, creating NaN values for missing records.

    - Load EPC data
    - Get best approximation for installation date
    - Merge EPC data with MCS installations data (and reformat)
    - Merge with MCS installers data
    - Reformat postcode
    - Save output to S3

    Args:
        epc_usecols (list, optional): Which EPC features to include. Defaults to base_config.EPC_PREPROC_FEAT_SELECTION.
        mcs_installations_usecols (list, optional): Which MCS installation features to include.
            Defaults to base_config.MCS_INSTALLATIONS_FEAT_SELECTION.
        mcs_installers_usecols (list, optional): Which MCS installer features to include.
            Defaults to base_config.MCS_INSTALLER_FEAT_SELECTION.
        data_path: local data path (defaults to "S3" but can be the local folder where ASF data is stored)
        verbose (bool, optional): Print shape of dataframes as they are merged (defaults to False).
    """

    # Load the processed EPC data (not deduplicated)
    merged_data = load_preprocessed_epc_data(
        data_path=path_to_data,
        version="preprocessed",
        batch="newest",
        usecols=epc_usecols,
        verbose=verbose,
    )

    # Add more precise estimations for heat pump installation dates via MCS data
    merged_data = install_date_computation.compute_hp_install_date(
        merged_data, verbose=verbose
    )

    # Merge EPC with MCS installations
    merged_data = add_mcs_installations_data(
        merged_data, usecols=mcs_installations_usecols, verbose=verbose
    )

    # Merge EPC/MCS with MCS installers
    merged_data = add_mcs_installer_data(merged_data, usecols=mcs_installers_usecols)

    # Add coordinates for EPC data
    merged_data = feature_engineering.get_postcode_coordinates(
        merged_data, postcode_field_name="POSTCODE"
    )

    today = date.today().strftime("%y%m%d")

    # Replacing all types of missing with NaN
    missing_values = ["Unknown", "unknown", "Undefined", "Unspecified", ""]
    for value in missing_values:
        merged_data.replace(value, np.nan, inplace=True)

    merged_data.reset_index(drop=True, inplace=True)

    # Save final merged dataset
    data_getters.save_to_s3(
        base_config.BUCKET_NAME,
        merged_data,
        base_config.EPC_MCS_MERGED_OUT_PATH.format(today),
    )


def create_argparser() -> ArgumentParser:
    """
    Creates an argument parser that can receive the following arguments:
    - path_to_data: either local path to where data is stored or "S3"
    - verbose: prints information while the pipeline is running if True
    """
    parser = ArgumentParser()

    parser.add_argument(
        "--path_to_data",
        help="Path to data",
        default="S3",
        type=str,
    )

    parser.add_argument(
        "--verbose",
        help="Verbose",
        default=False,
        type=bool,
    )

    return parser


if __name__ == "__main__":
    parser = create_argparser()
    args = parser.parse_args()

    merging_pipeline(path_to_data=args.path_to_data, verbose=args.verbose)
