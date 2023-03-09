# File: asf_core_data/pipeline/data_joining/merge_install_dates.py
"""Compute and update the heat pump installation dates
given EPC and MCS data."""

# ---------------------------------------------------------------------------------

from asf_core_data.getters import data_getters
from asf_core_data.config import base_config
from asf_core_data.getters.epc import data_batches

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------------


def reformat_mcs_date(mcs_df, feat):
    """Reformat dates in MCS/

    Args:
        mcs_df (pd.DataFrame): Dataframe to adjust.
        feat (str): Feature to adjust.

    Returns:
        pd.DataFrame: mcs_df with adjusted format.
    """

    mcs_df[feat] = pd.to_datetime(
        mcs_df[feat]
        .str.strip()
        .str.lower()
        .replace(r"\s.*", "", regex=True)
        .replace(r"-", "", regex=True),
        format="%Y%m%d",
    )

    return mcs_df


def get_mcs_install_date_mapping():
    """Retrieve MCS installation dates and create dictionary
    for mapping dates onto EPC records via UPRN.

    Returns:
        dict: Installation date dictionary derived from MCS.
    """

    newest_joined_batch = data_batches.get_latest_mcs_epc_joined_batch()

    # # Load MCS
    mcs_data = data_getters.load_s3_data(
        base_config.BUCKET_NAME,
        newest_joined_batch,
        usecols=[
            "commission_date",
            "tech_type",
            "version",
            "tech_type",
            "installation_type",
            "UPRN",
            "cluster",
            "installer_name",
            "postcode",
        ],
        dtype={"UPRN": "str"},
    )

    # Rename columns
    mcs_data.rename(
        columns={"commission_date": "HP_INSTALL_DATE"},
        inplace=True,
    )

    # Get the MCS install dates
    mcs_data = reformat_mcs_date(mcs_data, "HP_INSTALL_DATE")

    mcs_data = mcs_data.sort_values("HP_INSTALL_DATE", ascending=True).drop_duplicates(
        subset=["UPRN"], keep="first"
    )

    # Create a date dict from MCS data and apply to EPC data
    # If no install date is found for address, it assigns NaN
    mcs_hp_date_dict = mcs_data.set_index("UPRN").to_dict()["HP_INSTALL_DATE"]

    return mcs_hp_date_dict


def compute_hp_install_date(
    df,
    identifier="UPRN",
    verbose=False,
    add_hp_features=False,
):
    """Compute and update the heat pump installation date based on combined information from EPC and MCS.
    We get the best approximation for the installation date as follows:
    If there is an MCS installation record for this property, we use the HP commissioning date.
    If there isn't an MCS installation record, but EPC mentions having a heat pump, we consider
    the inspection date of the first EPC record for said property that mentions the heat pump.

    We also handle some edge cases, for example if an EPC record doesn't mention
    a heat pump at a time where there already should have been one according to MCS records.

    Check this sketch to understand the logic (numbers are outdated):
    https://user-images.githubusercontent.com/42718928/223767114-644802dd-07c9-4bb1-939f-b0c08d45b94a.png

    Note that this script doesn't merge EPC with MCS, it merely draws in MCS data for create a better estimate
    of the installation date.

    Args:
        df (pd.DataFrame): Dataframe with EPC data.
        identifier (str, optional): Unique identifier for properties. Defaults to "UPRN".
        verbose (bool, optional): Print some diagnostics. Defaults to True.
        add_hp_features (bool, optional): Compute additional features regarding mentions of heat pumps. Defaults to False.

    Returns:
        pd.DataFrame: Dataframe with updated install dates.
    """

    # Get the MCS install dates for EPC properties
    mcs_hp_date_dict = get_mcs_install_date_mapping()
    df["HP_INSTALL_DATE"] = df["UPRN"].map(mcs_hp_date_dict)

    df = df[df["INSPECTION_DATE"].notna()]

    # Get the first heat pump mention for each property
    first_hp_mention = (
        df.loc[df["HP_INSTALLED"]].groupby(identifier)["INSPECTION_DATE"].min()
    )

    df["FIRST_HP_MENTION"] = df[identifier].map(dict(first_hp_mention))

    # Additional features about heat pump history of property
    # Interesting to track unexpected behaviours, e.g. having lost a heat pump
    if add_hp_features:

        df["HP_AT_ANY_POINT"] = df[identifier].map(
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

    # If no first mention of HP, then set has no heat pump
    df["HAS_HP_AT_SOME_POINT"] = ~df["FIRST_HP_MENTION"].isna()

    # HP entry conditions
    no_mcs_or_epc = (~df["MCS_AVAILABLE"]) & (~df["HP_INSTALLED"])
    no_mcs_but_epc_hp = (~df["MCS_AVAILABLE"]) & (df["HP_INSTALLED"])
    mcs_and_epc_hp = (df["MCS_AVAILABLE"]) & (df["HP_INSTALLED"])
    no_epc_but_mcs_hp = (df["MCS_AVAILABLE"]) & (~df["HP_INSTALLED"])
    either_hp = (df["MCS_AVAILABLE"]) | (df["HP_INSTALLED"])
    epc_entry_before_mcs = df["INSPECTION_DATE"] < df["HP_INSTALL_DATE"]

    # We use this tag to track whether a record was artificially duplicated
    # to handle an edge case (see below:  MCS but no EPC HP with EPC HP mention before MCS)
    df["ARTIFICIALLY_DUPL"] = False

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

    df["HP_INSTALLED"] = np.where(
        (no_epc_but_mcs_hp & epc_entry_before_mcs), False, df["HP_INSTALLED"]
    )
    df["HP_INSTALL_DATE"] = np.where(
        (no_epc_but_mcs_hp & epc_entry_before_mcs),
        np.datetime64("NaT"),
        df["HP_INSTALL_DATE"],
    )

    df = pd.concat([df, no_future_hp_entry])

    # Deduplicate and only keep latest record
    # Note: Depending on the use case, you may want to filter differently
    # (e.g. when looking at the property characteristcs just before installing a heat pump
    df = df.sort_values("INSPECTION_DATE", ascending=True).drop_duplicates(
        subset=[identifier], keep="last"
    )

    return df
