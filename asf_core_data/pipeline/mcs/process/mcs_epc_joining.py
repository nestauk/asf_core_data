# File: heat_pump_adoption_modelling/pipeline/mcs/process/mcs_epc_joining.py
"""Joining the MCS and EPC datasets.
Overall process is as follows:
- Standardise address and postcode fields
- Extract numeric tokens from address
- Group by postcode
- Exact match on numeric tokens
- Compare address using Jaro-Winkler score
- Drop any match with score below a certain parameter
- Of the remaining matches, take the ones with highest score
- Join datasets using this matching
"""

import recordlinkage as rl

from asf_core_data import PROJECT_DIR, get_yaml_config
from asf_core_data.pipeline.mcs.process.process_mcs_installations import (
    get_processed_installations_data,
)
from asf_core_data.getters.epc.epc_data import (
    load_preprocessed_epc_data,
)
from asf_core_data.getters.data_getters import load_s3_data
from asf_core_data.pipeline.mcs.process.process_mcs_utils import (
    remove_punctuation,
    extract_token_set,
)

config = get_yaml_config(PROJECT_DIR / "asf_core_data/config/base.yaml")

max_token_length = config["MCS_EPC_MAX_TOKEN_LENGTH"]
matching_parameter = config["MCS_EPC_MATCHING_PARAMETER"]
bucket_name = config["BUCKET_NAME"]

# ---------------------------------------------------------------------------------

#### PREPROCESSING


def prepare_hps(hps):
    """Prepare Dataframe of HP installations by adding
    standardised_postcode, standardised_address and numeric_tokens fields.

    Args:
        hps (Dataframe): Dataframe with postcode, address_1 and address_2 fields.

    Returns:
        Dataframe: Domestic HP records with added fields.
    """

    hps["standardised_postcode"] = (
        hps["postcode"].fillna("unknown").str.upper().str.replace(" ", "")
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

    Args:
        epcs (Dataframe): EPC records with POSTCODE, ADDRESS1 and ADDRESS2 fields.

    Returns:
        Dataframe: EPC records with added fields.
    """

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
        extract_token_set(address, postcode, max_token_length)
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

    Args:
        df1 (Dataframe): Dataframe with standardised_postcode,
        numeric_tokens and standardised_address fields.
        df2 (Dataframe): Dataframe with standardised_postcode,
        numeric_tokens and standardised_address fields.

    Returns:
        Dataframe: Indices of df1 and matched indices in df2
        along with similarity scores for numeric tokens (value in {0, 1})
        and address (continuous value in [0, 1]).
    """

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


def join_prepared_mcs_epc_data(
    hps,
    epcs,
    all_records=True,
    drop_epc_address=True,
    verbose=True,
):
    """Join prepared MCS and EPC data.

    Args:
        hps (Dataframe): Dataframe with standardised_postcode,
        numeric_tokens and standardised_address fields.
        epcs (Dataframe): Dataframe with standardised_postcode,
        numeric_tokens and standardised_address fields.
        all_records (bool, optional): Whether all top matches should be kept,
        or just one. Keeping all records allows for comparison of
        property characteristics over time. Defaults to True.
        drop_epc_address (bool, optional): Whether or not to drop addresses
        from the EPC records. Useful to keep for determining whether
        matches are sensible. Defaults to True.
        verbose (bool, optional): Whether or not to print diagnostic information
        about the matching, e.g. number of matched records. Defaults to True.

    Returns:
        Dataframe: Merged MCS and EPC records.
    """

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
                "ADDRESS2",
                "POSTTOWN",
                "POSTCODE",
                "index_y",
                "postcode_y",
                "standardised_postcode_y",
                "numeric_tokens_y",
                "level_0",
            ],
            errors="ignore",
        )
    )
    merged = merged.rename(
        columns={
            "index_x": "original_mcs_index",
            "level_1": "original_epc_index",
            "postcode_x": "postcode",
        }
    )

    if drop_epc_address:
        merged = merged.drop(columns="standardised_address_y")
    else:
        merged = merged.rename(columns={"standardised_address_y": "epc_address"})

    if verbose:

        print("After joining:\n-----------------")
        print("Total records:", merged.shape[0])
        print(
            "Number matched with EPC:",
            merged.loc[~merged["original_epc_index"].isna()].shape[0],
        )
        print("\n")

    return merged


def join_mcs_epc_data(
    hps=None, epcs=None, all_records=True, drop_epc_address=True, verbose=True
):
    """Produce joined MCS-EPC dataframe from "unprepared" data.

    Args:
        hps (Dataframe, optional): MCS installation records.
        If None, records are fetched automatically. Defaults to None.
        epcs (Dataframe, optional): EPC records. If None, records
        are fetched automatically. Defaults to None.
        all_records (bool, optional): Whether or not to use all matching EPC
        records or just take one. Defaults to True.
        drop_epc_address (bool, optional): Whether or not to drop the address
        from the EPC records used for matching. Defaults to True.
        verbose (bool, optional): Whether or not to print diagnostic information.
        Defaults to True.

    Returns:
        Dataframe: Matched MCS-EPC records.
    """
    if hps is None:
        print("Getting HP data...")
        hps = get_processed_installations_data()

    if epcs is None:
        epc_version = "preprocessed" if all_records else "preprocessed_dedupl"
        print("Getting EPC data...")
        # TODO: replace with line that loads most recent full EPC data
        epcs = load_s3_data(
            bucket_name,
            "outputs/EPC/EPC_GB_preprocessed_and_deduplicated_sample_prox.csv",
            usecols=None,
        )

    prepared_hps = prepare_hps(hps)
    prepared_epcs = prepare_epcs(epcs)

    joined = join_prepared_mcs_epc_data(
        prepared_hps,
        prepared_epcs,
        all_records=all_records,
        drop_epc_address=drop_epc_address,
        verbose=verbose,
    )

    return joined


def select_most_relevant_epc(joined_df):
    """From a "fully joined" MCS-EPC dataset, chooses the "best"
    EPC for each HP installation record (i.e. the one that is assumed
    to best reflect the status of the property at the time of HP installation).
    The EPC chosen is the latest one before the installation if it exists;
    otherwise it is the earliest one after the installation.

    Args:
        joined_df (Dataframe): Joined MCS-EPC data. Assumed to
        contain INSPECTION_DATE, commission_date and original_mcs_index columns.

    Returns:
        Dataframe: Most relevant MCS-EPC records.
    """

    # Sort joined_df by INSPECTION_DATE
    joined_df = joined_df.sort_values("INSPECTION_DATE").reset_index(drop=True)

    # Identify rows where the EPC data is before the MCS one
    joined_df["epc_before_mcs"] = (
        joined_df["INSPECTION_DATE"] <= joined_df["commission_date"]
    )

    # Identify indices of last EPC record before MCS
    last_epc_before_mcs_indices = (
        joined_df.reset_index()
        .loc[joined_df["epc_before_mcs"]]
        .groupby("original_mcs_index")
        .tail(1)["index"]
        .values
    )

    joined_df["last_epc_before_mcs"] = False
    joined_df["last_epc_before_mcs"].iloc[last_epc_before_mcs_indices] = True

    # Filter to either "last EPC before MCS" or "EPC after MCS",
    # then group by installation and take first record -
    # this will be the last EPC before MCS
    # if it exists, otherwise the first EPC after MCS
    filtered_data = (
        joined_df.loc[joined_df["last_epc_before_mcs"] | ~joined_df["epc_before_mcs"]]
        .groupby("original_mcs_index")
        .head(1)
        .reset_index(drop=True)
        .drop(columns=["epc_before_mcs", "last_epc_before_mcs"])
    )

    return filtered_data


#### For testing purposes:

# import pandas as pd

# test_mcs = pd.DataFrame({
#     "postcode": ["A1 1AA", "A1 1AB", "A1 1AD"],
#     "address_1": ["1 Main Street", "2 High Street", "4 Side Avenue"],
#     "address_2": ["Townsville", "Cityburgh", "Townsville"],
#     "cost": [10500, 8500, 9000],
# })

# test_epc = pd.DataFrame({
#     "POSTCODE": ["A1 1AB", "A1 1AA", "A1 1AC", "A1 1AB", "A1 1AA", "A1 1AA"],
#     "ADDRESS1": ["2 High Street", "1 Main Street", "2 High Street", "3 High Street", "1 Main Street", "1 The House"],
#     "ADDRESS2": ["Cityburgh", "Townsville", "Cityburgh", "Cityburgh", "Townsville", "Townsville"],
#     "ENERGY_EFFICIENCY_RATING": ["D", "A", "C", "D", "B", "F"],
# })

# prepared_hps = prepare_hps(test_mcs)
# prepared_epcs = prepare_epcs(test_epc)

# join_mcs_epc_data(hps=test_mcs, epcs=test_epc, all_records=False)
