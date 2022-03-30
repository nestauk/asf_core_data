# File: heat_pump_adoption_modelling/pipeline/preprocessing/mcs_epc_joining.py
"""Joining the MCS and EPC datasets.
Overall process is as follows:
- Standardise address and postcode fields
- Extract numeric tokens from address
- Group by postcode
- Exact match on numeric tokens
- Compare address using Jaro-Winkler score
- Drop any match with score below a certain parameter
- Of the remaining matches, take the one with highest score
- Join datasets using this matching
"""

import pandas as pd
import numpy as np
import re
import recordlinkage as rl
import time

from asf_core_data import PROJECT_DIR, get_yaml_config, Path
from asf_core_data.pipeline.mcs.process.process_mcs_installations import (
    get_processed_mcs_data,
)
from asf_core_data.getters.epc.get_epc import load_preprocessed_epc_data
from asf_core_data.pipeline.mcs.process.process_mcs_utils import (
    remove_punctuation,
    extract_token_set,
)

config = get_yaml_config(Path(str(PROJECT_DIR) + "/asf_core_data/config/base.yaml"))

max_token_length = config["MCS_EPC_MAX_TOKEN_LENGTH"]
address_fields = config["MCS_EPC_ADDRESS_FIELDS"]
characteristic_fields = config["MCS_EPC_CHARACTERISTIC_FIELDS"]
matching_parameter = config["MCS_EPC_MATCHING_PARAMETER"]
merged_path = config["MCS_EPC_MERGED_PATH"]
supervised_model_features = config["EPC_PREPROC_FEAT_SELECTION"]  # needed?

# ---------------------------------------------------------------------------------

#### PREPROCESSING


def prepare_hps(hps):
    """Prepare Dataframe of HP installations by adding
    standardised_postcode, standardised_address and numeric_tokens fields.
    Parameters
    ----------
    dhps : pandas.Dataframe
        Dataframe with postcode, address_1 and address_2 fields.
    Return
    ----------
    dhps : pandas.Dataframe
        Dataframe containing domestic HP records with added fields."""

    hps["standardised_postcode"] = (
        hps["postcode"].fillna("unknown").str.upper().str.strip()
    )

    hps["standardised_address"] = [
        # Make address 1 and 2 lowercase, strip whitespace,
        # and combine into a single string separated by a space
        remove_punctuation(add1).lower().strip()
        + " "
        + remove_punctuation(add2).lower().strip()
        for add1, add2 in zip(hps["address_1"].fillna(""), hps["address_2"].fillna(""))
    ]

    hps["numeric_tokens"] = [
        extract_token_set(address, postcode, max_token_length)
        for address, postcode in zip(
            hps["standardised_address"].fillna(""), hps["postcode"].fillna("")
        )
    ]

    return hps


def prepare_epcs(epcs):
    """Prepare Dataframe of EPC records by adding
    standardised_postcode, standardised_address and numeric_tokens fields.
    Parameters
    ----------
    epcs : pandas.Dataframe
        Dataframe with POSTCODE, ADDRESS1 and ADDRESS2 fields.
    Return
    ----------
    epcs : pandas.Dataframe
        Dataframe containing EPC records with added fields."""

    # Keep original EPC address
    epcs["compressed_epc_address"] = (
        epcs["ADDRESS1"] + epcs["ADDRESS2"] + epcs["POSTCODE"]
    )

    # Remove white spaces
    epcs["compressed_epc_address"] = (
        epcs["compressed_epc_address"]
        .str.strip()
        .str.lower()
        .replace(r"\s+", "", regex=True)
    )

    # Remove spaces, uppercase and strip whitespace from
    # postcodes in order to exact match on this field
    epcs["standardised_postcode"] = (
        epcs["POSTCODE"].fillna("UNKNOWN").str.upper().str.replace(" ", "")
    )

    # Remove punctuation, lowercase and concatenate address fields
    # for approximate matching
    epcs["standardised_address"] = [
        remove_punctuation(address).lower().strip() for address in epcs["ADDRESS1"]
    ]

    epcs["numeric_tokens"] = [
        extract_token_set(address, postcode)
        for address, postcode in zip(
            epcs["standardised_address"].fillna(""), epcs["POSTCODE"].fillna("")
        )
    ]

    return epcs


# ---------------------------------------------------------------------------------


#### JOINING


def form_matching(df1, df2):
    """Form a matching between two Dataframes.
    Initially an index is formed between records with shared
    standardised_postcode, then the records are compared for
    exact matches on numeric tokens and fuzzy matches on
    standardised_address using Jaro-Winkler method.
    Jaro-Winkler chosen here as it prioritises characters
    near the start of the string - this feels suitable as
    the key information (such as house name) is likely to
    be near the start of the string
    This feels better suited than e.g. Levenshtein as to not
    excessively punish the inclusion of extra information at
    the end of the address field e.g. town, county.
    Parameters
    ----------
    df1 : pandas.Dataframe
        Dataframe with standardised_postcode, numeric_tokens
        and standardised_address fields.
    df2 : pandas.Dataframe
        Dataframe with standardised_postcode, numeric_tokens
        and standardised_address fields.
    Return
    ----------
    matching : pandas.Dataframe
        Dataframe giving indices of df1 and matched indices in df2
        along with similarity scores for numeric tokens (value in {0, 1})
        and address (continuous value in [0, 1])."""

    # Index
    print("- Forming an index...")
    indexer = rl.Index()
    indexer.block(on="standardised_postcode")
    candidate_pairs = indexer.index(df1, df2)

    # Compare
    print("- Forming a comparison...")
    comp = rl.Compare()
    comp.exact("numeric_tokens", "numeric_tokens", label="numerics")
    comp.string(
        "standardised_address",
        "standardised_address",
        method="jarowinkler",
        label="address_score",
    )

    # Classify
    print("- Computing a matching...")
    matching = comp.compute(candidate_pairs, df1, df2)

    return matching


def join_mcs_epc_data(
    hps=None,
    epcs=None,
    save=True,
    all_records=False,
    drop_epc_address=False,
    verbose=True,
):
    """Join MCS and EPC data.
    Parameters
    ----------
    hps : pandas.Dataframe
        Dataframe with standardised_postcode, numeric_tokens
        and standardised_address fields.
        If None, HP data is loaded and augmented.
    epcs : pandas.Dataframe
        Dataframe with standardised_postcode, numeric_tokens
        and standardised_address fields.
        If None, EPC data is loaded and augmented.
    all_records : bool
        Whether all top matches should be kept, or just one.
        Keeping all records allows for comparison of property
        characteristics over time.
    save : bool
        Whether or not to save the output.
    drop_epc_address : bool
        Whether or not to drop addresses from the EPC records.
        Useful to keep for determining whether matches are sensible.
    Return
    ----------
    merged : pandas.Dataframe
        Dataframe containing merged MCS and EPC records."""

    if hps is None:
        print("Preparing HP data...")
        hps = get_processed_mcs_data()
        hps = prepare_hps(hps)

    if epcs is None:
        epc_version = "preprocessed" if all_records else "preprocessed_dedupl"
        print("Preparing EPC data...")
        fields_of_interest = address_fields + characteristic_fields
        epcs = load_preprocessed_epc_data(
            version=epc_version, usecols=fields_of_interest, low_memory=True
        )
        epcs = prepare_epcs(epcs)

    print("Forming a matching...")
    matching = form_matching(df1=hps, df2=epcs)

    # First ensure that all matches are above the matching parameter
    good_matches = matching[
        (matching["numerics"] == 1) & (matching["address_score"] >= matching_parameter)
    ].reset_index()

    if all_records:
        top_matches = (
            good_matches.groupby("level_0")
            # Get all level_1 indices of rows in which address_score is maximal
            .apply(
                lambda df: df.loc[
                    df["address_score"] == df["address_score"].max(), "level_1"
                ]
            )
            .droplevel(1)
            .reset_index()
            .rename(columns={"index": "level_0"})
        )

    else:
        # Get the level_1 index of the first occurrence at which address_score is maximal
        top_match_indices = good_matches.groupby("level_0")["address_score"].idxmax()

        # Filter the matches df to just the top matches
        top_matches = good_matches.loc[top_match_indices].drop(
            columns=["numerics", "address_score"]
        )

    print("Joining the data...")
    merged = (
        hps.reset_index()
        # Join MCS records to the index-matching df on MCS index
        .merge(top_matches, how="left", left_on="index", right_on="level_0")
        # Then join this merged df to EPC records on EPC index
        .merge(epcs.reset_index(), how="left", left_on="level_1", right_on="index")
        # Drop any duplicated or unnecessary columns
        .drop(
            columns=[
                "standardised_postcode_x",
                "standardised_address_x",
                "numeric_tokens_x",
                "ADDRESS1",
                "POSTTOWN",
                "POSTCODE",
                "index_y",
                "standardised_postcode_y",
                "numeric_tokens_y",
            ]
        )
    )

    if drop_epc_address:
        merged = merged.drop(columns="standardised_address_y")
    else:
        merged = merged.rename(columns={"standardised_address_y": "epc_address"})

    if verbose:

        print("After joining:\n-----------------\n")
        # print(merged["installation_type"].value_counts(dropna=False))
        print(merged.shape)

        print(
            "Matched with EPC",
            merged.loc[~merged["compressed_epc_address"].isna()].shape,
        )
        print(
            "Not matched with EPC",
            merged.loc[merged["compressed_epc_address"].isna()].shape,
        )

    merged["# records"] = merged["compressed_epc_address"].map(
        dict(merged.groupby("compressed_epc_address").count()["date"])
    )

    if verbose:
        pass
        # print(
        #     merged.loc[merged["compressed_epc_address"].isna()][
        #         "installation_type"
        #     ].value_counts(dropna=False)
        # )

        # print(
        #     merged.loc[~merged["compressed_epc_address"].isna()][
        #         "installation_type"
        #     ].value_counts(dropna=False)
        # )

    if not all_records:
        merged = merged.sort_values("date", ascending=True).drop_duplicates(
            subset=["compressed_epc_address"], keep="first"
        )
        if verbose:
            print(merged.shape)

        merged = merged[merged["compressed_epc_address"].notna()]
        if verbose:
            print(merged.shape)

            print("After removing duplicates:\n-----------------\n")
            #  print(merged["installation_type"].value_counts(dropna=False))
            print(merged.shape)

            print(merged.loc[merged["compressed_epc_address"].isna()].shape)

            print(merged.shape)

    if save:
        merged.to_csv(str(PROJECT_DIR) + merged_path)

    return merged


def select_most_relevant_epc(data):
    """From a "fully joined" MCS-EPC dataset, chooses the "best"
    EPC for each HP installation record (i.e. the one that is assumed
    to best reflect the status of the property at the time of HP installation).
    The EPC chosen is the latest one before the installation if it exists;
    otherwise it is the earliest one after the installation.

    Args:
        data ([type]): Joined MCS-EPC data. Assumed to
        contain INSPECTION_DATE, date and index_x columns.

    Returns:
        Dataframe: HP installation records attached to the
    """

    # Sort data by INSPECTION_DATE
    data = data.sort_values("INSPECTION_DATE").reset_index(drop=True)

    # Identify rows where the EPC data is before the MCS one
    data["epc_before_mcs"] = data["INSPECTION_DATE"] <= data["date"]

    # Identify indices of last EPC record before MCS
    last_epc_before_mcs_indices = (
        data.reset_index()
        .loc[data["epc_before_mcs"]]
        .groupby("index_x")
        .tail(1)["index"]
        .values
    )

    data["last_epc_before_mcs"] = False
    data["last_epc_before_mcs"].iloc[last_epc_before_mcs_indices] = True

    # Filter to either "last EPC before MCS" or "EPC after MCS",
    # then group by installation and take first record -
    # this will be the last EPC before MCS
    # if it exists, otherwise the first EPC after MCS
    filtered_data = (
        data.loc[data["last_epc_before_mcs"] | ~data["epc_before_mcs"]]
        .groupby("index_x")
        .head(1)
        .reset_index(drop=True)
        .drop(columns=["epc_before_mcs", "last_epc_before_mcs"])
    )

    return filtered_data


# ---------------------------------------------------------------------------------


def main():
    """Main function: Load and join MCS data to EPC data."""

    start_time = time.time()

    join_mcs_epc_data()

    end_time = time.time()
    runtime = round((end_time - start_time) / 60)

    print(
        "Loading and joining MCS and EPC data took {} minutes.\n\nOutput saved in {}".format(
            runtime, merged_path
        )
    )


if __name__ == "__main__":
    # Execute only if run as a script
    main()
