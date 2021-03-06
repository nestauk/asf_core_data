# File: asf_core_data/pipeline/data_joining/merge_install_dates.py
"""Merge HP install date information from EPC and MCS."""

# ---------------------------------------------------------------------------------

from asf_core_data.getters import data_getters
from asf_core_data.config import base_config

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------------


def get_mcs_install_dates(epc_df, additional_features=False):
    """Get MCS install dates and add them to the EPC data.

    Args:
        epc_df (pandas.DataFrame): EPC dataset.
        additional_features (bool, optional): Whether to add additional MCS features. Defaults to False.

    Returns:
        pandas.DataFrame: EPC dataset with added MCS install dates.
    """

    # Note
    # TO DO:  This will be simplified shortly to work with UPRN instead of address

    # Get original address from EPC
    epc_df["original_address"] = (
        epc_df["ADDRESS1"] + epc_df["ADDRESS2"] + epc_df["POSTCODE"]
    )
    epc_df["original_address"] = (
        epc_df["original_address"]
        .str.strip()
        .str.lower()
        .replace(r"\s+", "", regex=True)
    )

    # Load MCS
    mcs_data = data_getters.load_s3_data(
        base_config.BUCKET_NAME,
        base_config.MCS_EPC_MERGED_PATH,
        usecols=[
            "date",
            "tech_type",
            "compressed_epc_address",
            "address_1",
            "address_2",
            "address_3",
            "postcode",
            "version",
            "alt_type",
            "installation_type",
            "# records",
        ],
    )

    mcs_data.fillna({"address_1": "", "address_2": "", "address_3": ""}, inplace=True)

    # Rename columns
    mcs_data.rename(
        columns={
            "date": "HP_INSTALL_DATE",
            "tech_type": "Type of HP",
            "compressed_epc_address": "compressed_epc_address",
            "address_1": "MCS address 1",
            "address_2": "MCS address 2",
            "address_3": "MCS address 3",
            "postcode": "MCS postcode",
        },
        inplace=True,
    )

    # Get original EPC address from MCS/EPC match
    mcs_data = mcs_data.loc[~mcs_data["compressed_epc_address"].isna()]
    mcs_data["MCS_ADDRESS"] = (
        mcs_data["MCS address 1"]
        + " "
        + mcs_data["MCS address 2"]
        + " "
        + mcs_data["MCS address 3"]
        + " "
        + mcs_data["MCS postcode"]
    )

    mcs_data["compressed_epc_address"] = (
        mcs_data["compressed_epc_address"]
        .str.strip()
        .str.lower()
        .replace(r"\s+", "", regex=True)
    )

    # Get the MCS install dates
    mcs_data["HP_INSTALL_DATE"] = pd.to_datetime(
        mcs_data["HP_INSTALL_DATE"]
        .str.strip()
        .str.lower()
        .replace(r"-", "", regex=True),
        format="%Y%m%d",
    )

    # no Nans or "" in compressed_epc_address
    mcs_data = mcs_data.sort_values("HP_INSTALL_DATE", ascending=True).drop_duplicates(
        subset=["compressed_epc_address"], keep="first"
    )

    # Create a date dict from MCS data and apply to EPC data
    # If no install date is found for address, it assigns NaN
    date_dict = mcs_data.set_index("compressed_epc_address").to_dict()[
        "HP_INSTALL_DATE"
    ]

    original_address_dict = mcs_data.set_index("compressed_epc_address").to_dict()[
        "MCS_ADDRESS"
    ]

    epc_df["HP_INSTALL_DATE"] = epc_df["original_address"].map(date_dict)
    epc_df["MCS address"] = epc_df["original_address"].map(original_address_dict)

    if additional_features:
        for feat in [
            "version",
            "alt_type",
            "installation_type",
            "# records",
        ]:

            epc_df[feat] = epc_df["original_address"].map(
                mcs_data.set_index("compressed_epc_address").to_dict()[feat]
            )

    return epc_df


def manage_hp_install_dates(df, identifier="UPRN", verbose=False):
    """Manage heat pump install dates given by EPC and MCS.

    Args:
        df (pd.DataFrame): Dataframe with EPC data and MCS install dates.
        identifier (str, optional): Unique identifier for properties. Defaults to "UPRN".
        verbose (bool, optional): Print some diagnostics. Defaults to True.

    Returns:
        pd.DataFrame: Dataframe with updated install dates.
    """

    # Get the MCS install dates for EPC properties
    df = get_mcs_install_dates(df)

    df = df[df["INSPECTION_DATE"].notna()]

    # Get the first heat pump mention for each property
    first_hp_mention = (
        df.loc[df["HP_INSTALLED"]].groupby(identifier)["INSPECTION_DATE"].min()
    )

    df["FIRST_HP_MENTION"] = df[identifier].map(dict(first_hp_mention))

    df["ANY_HP"] = df[identifier].map(
        dict(df.groupby(identifier)["HP_INSTALLED"].max())
    )

    df["HP_AT_FIRST"] = df[identifier].map(
        df.loc[df.groupby(identifier)["INSPECTION_DATE"].idxmin()]
        .set_index(identifier)
        .to_dict()["HP_INSTALLED"]
    )

    df["HP_AT_LAST"] = df[identifier].map(
        df.loc[df.groupby(identifier)["INSPECTION_DATE"].idxmax()]
        .set_index(identifier)
        .to_dict()["HP_INSTALLED"]
    )

    df["HP_LOST"] = df["HP_AT_FIRST"] & ~df["HP_AT_LAST"]
    df["HP_ADDED"] = ~df["HP_AT_FIRST"] & df["HP_AT_LAST"]
    df["HP_IN_THE_MIDDLE"] = (
        ~df["HP_AT_FIRST"] & ~df["HP_AT_LAST"] & ~df["FIRST_HP_MENTION"].isna()
    )

    # If no HP Install date, set MCS availabibility to False
    df["MCS_AVAILABLE"] = ~df["HP_INSTALL_DATE"].isna()

    # If no first mention of HP, then set has
    df["HAS_HP_AT_SOME_POINT"] = ~df["FIRST_HP_MENTION"].isna()
    df["ARTIFICIALLY_DUPL"] = False

    # HP entry conditions
    no_mcs_or_epc = (~df["MCS_AVAILABLE"]) & (~df["HP_INSTALLED"])
    no_mcs_but_epc_hp = (~df["MCS_AVAILABLE"]) & (df["HP_INSTALLED"])
    mcs_and_epc_hp = (df["MCS_AVAILABLE"]) & (df["HP_INSTALLED"])
    no_epc_but_mcs_hp = (df["MCS_AVAILABLE"]) & (~df["HP_INSTALLED"])
    either_hp = (df["MCS_AVAILABLE"]) | (df["HP_INSTALLED"])
    epc_entry_before_mcs = df["INSPECTION_DATE"] < df["HP_INSTALL_DATE"]

    if verbose:

        print("Total", df.shape[0])
        print("MCS and EPC", df[mcs_and_epc_hp].shape[0])
        print("no MCS or EPC", df[no_mcs_or_epc].shape[0])
        print("either", df[either_hp].shape[0])
        print("no MCS but EPC", df[no_mcs_but_epc_hp].shape[0])
        print("no epc but mcs", df[no_epc_but_mcs_hp].shape[0])

        print(
            "no HP mention: epc_entry_before mcs",
            df[no_epc_but_mcs_hp & epc_entry_before_mcs].shape[0],
        )

        print(
            "no HP mention: epc_entry_after/same mcs",
            df[no_epc_but_mcs_hp & ~epc_entry_before_mcs].shape[0],
        )

        print(
            "EPC HP mention: epc_entry_before mcs",
            df[mcs_and_epc_hp & epc_entry_before_mcs].shape[0],
        )

        print(
            "EPC HP mention: epc_entry_after/same mcs",
            df[mcs_and_epc_hp & ~epc_entry_before_mcs].shape[0],
        )

    # Handling the various cases
    # For completeness' sake, all cases are listed
    # although not all require changes to the data.
    # ---------------------------

    # NO MCS/EPC HP entry
    # --> No heat pump, no install date
    # No changes to data required.

    # -----

    # No MCS entry but EPC HP entry
    # --> HP: yes (already set), install date: first HP mention

    df["HP_INSTALL_DATE"] = np.where(
        no_mcs_but_epc_hp, df["FIRST_HP_MENTION"], df["HP_INSTALL_DATE"]
    )

    # -----

    # MCS and EPC HP entry, with EPC entry after MCS installatioon
    # --> HP: yes,  install date: MCS install date
    # No changes to data required.

    # -----

    # MCS and EPC HP entry, EPC entry before MCS installatioon
    # ! We want to discard that option as it should not happen!
    # Set HP to False and Install Date to NA

    df["EPC HP entry before MCS"] = np.where(
        (mcs_and_epc_hp & epc_entry_before_mcs), True, False
    )

    # Handling edge case
    df["HP_INSTALLED"] = np.where(
        (mcs_and_epc_hp & epc_entry_before_mcs), False, df["HP_INSTALLED"]
    )
    df["HP_INSTALL_DATE"] = np.where(
        (mcs_and_epc_hp & epc_entry_before_mcs),
        np.datetime64("NaT"),
        df["HP_INSTALL_DATE"],
    )

    # -----

    # MCS but no EPC HP, with EPC entry after MCS instalation
    # Should not happen as EPC after MCs installation should show HP!

    df["No EPC HP entry after MCS"] = np.where(
        (no_epc_but_mcs_hp & ~epc_entry_before_mcs), True, False
    )

    # Handling edge case
    df["HP_INSTALLED"] = np.where(
        (no_epc_but_mcs_hp & ~epc_entry_before_mcs), False, df["HP_INSTALLED"]
    )
    df["HP_INSTALL_DATE"] = np.where(
        (no_epc_but_mcs_hp & ~epc_entry_before_mcs),
        np.datetime64("NaT"),
        df["HP_INSTALL_DATE"],
    )

    df["HP_INSTALLED"] = np.where(
        (no_epc_but_mcs_hp & ~epc_entry_before_mcs), True, df["HP_INSTALLED"]
    )

    # ---
    # MCS but no EPC HP with EPC HP mention before MCS
    # Set current instance to no heat pump
    # but create duplicate with MCS install date if no future EPC HP mention

    # Get samples for which there is no future EPC HP mention
    # and create duplicate with MCS install data

    no_future_hp_entry = df[
        no_epc_but_mcs_hp & epc_entry_before_mcs & (df["HAS_HP_AT_SOME_POINT"] == False)
    ].copy()

    # Update heat pump data and add newly created entries to other data
    no_future_hp_entry["HP_INSTALLED"] = True
    no_future_hp_entry["HAS_HP_AT_SOME_POINT"] = True
    no_future_hp_entry["INSPECTION_DATE"] = no_future_hp_entry["HP_INSTALL_DATE"]
    no_future_hp_entry["ARTIFICIALLY_DUPL"] = True

    # df["HP_INSTALLED"] = np.where(
    #     (no_epc_but_mcs_hp & epc_entry_before_mcs), False, df["HP_INSTALLED"]
    # )
    df["HP_INSTALL_DATE"] = np.where(
        (no_epc_but_mcs_hp & epc_entry_before_mcs),
        np.datetime64("NaT"),
        df["HP_INSTALL_DATE"],
    )

    df = pd.concat([df, no_future_hp_entry])

    return df
