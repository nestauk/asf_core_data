# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     comment_magics: true
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: asf_core_data_kernel
#     language: python
#     name: asf_core_data_kernel
# ---

# %%
import pandas as pd
import os
from datetime import datetime

# %%
from asf_core_data.pipeline.mcs.process import mcs_epc_joining as joining

# %% [markdown]
# ## Set local_core_data_dir where EPC data is stored

# %%
## Set your local ASF core data dir
local_core_data_dir = ""

## Output data will be saved here
outputs_data_dir = os.path.abspath("../../outputs/data")

# %% [markdown]
# ## Read in data
#
# EPC data: 2023 Q2 complete, GB dedupl and preprocessed\
# MCS data: mcs_installations_231009

# %%
epc_data = pd.read_csv(
    os.path.join(
        local_core_data_dir,
        "outputs/EPC/preprocessed_data/2023_Q2_complete/EPC_GB_preprocessed_and_deduplicated.csv",
    )
)
mcs_data = pd.read_csv("s3://asf-core-data/outputs/MCS/mcs_installations_231009.csv")

# %%
print(epc_data.shape)
print(mcs_data.shape)

# %% [markdown]
# ## Prepare data for address matching

# %%
epc_df = joining.prepare_epcs(epc_data)

# %%
mcs_df = joining.prepare_hps(mcs_data)

# %% [markdown]
# ## Identify potential address matches
#
# Here we calculate an address match score for all candidate matches (i.e. all addresses in the same postcode).

# %%
matches = joining.form_matching(mcs_df, epc_df)

# %%
print(len(matches))

# %% [markdown]
# ## Join walkthrough
#
# Here we will join EPC data to MCS data via address matching using the function used in the asf_core_data pipeline. The function is modified here to retain standardised joining cols and address score. All top matches of EPC to MCS (equivalent to setting all_records=True) will be kept.

# %%
# Filter to good matches only
# Good matches are defined as those with matching numeric tokens and an address matching score >= 0.7, as in the asf_core_data pipeline
good_matches = matches[
    (matches["numerics"] == 1) & (matches["address_score"] >= 0.7)
].reset_index()

# %%
top_matches = (
    good_matches.groupby("level_0")
    # Get all level_1 indices of rows in which address_score is maximal
    .apply(
        lambda df: df.loc[
            df["address_score"] == df["address_score"].max(),
            ["level_1", "address_score"],
        ]
    )
    .droplevel(1)
    .reset_index()
    .rename(columns={"index": "level_0"})
)

# %%
# Keep standardised address, postcode and numeric tokens columns for reference later

hps = mcs_df.copy()
epcs = epc_df.copy()

hps = hps.rename(
    columns={
        "standardised_address": "mcs_address",
        "standardised_postcode": "mcs_postcode",
        "numeric_tokens": "mcs_numeric_tokens",
    }
)
epcs = epcs.rename(
    columns={
        "standardised_address": "epc_address",
        "standardised_postcode": "epc_postcode",
        "numeric_tokens": "epc_numeric_tokens",
    }
)

# %%
# Prepare mcs dataset for left merge
hps = hps.reset_index()

# %%
# Join the index-matching df on MCS index to the MCS records
merged = hps.merge(top_matches, how="left", left_on="index", right_on="level_0")

# %%
# Join the EPC data onto the MCS records
merged = merged.merge(
    epcs.reset_index().drop(
        columns=[
            "ADDRESS1",
            "ADDRESS2",
            "POSTTOWN",
            "POSTCODE",
        ],
        errors="ignore",
    ),
    how="left",
    left_on="level_1",
    right_on="index",
)

# %%
# Clean up the df
merged = merged.drop(
    columns=[
        "index_y",
        "level_0",
    ],
    errors="ignore",
)

merged = merged.rename(
    columns={
        "index_x": "original_mcs_index",
        "level_1": "original_epc_index",
        "postcode_x": "postcode",
    }
)

# %% [markdown]
# ## Check output is the same with pipeline function - join datasets
# Check with the joining function to make sure the join has been correctly replicated.

# %%
# See matching with existing address-matching function
mcs_epc_join_function = joining.join_prepared_mcs_epc_data(
    hps=mcs_df, epcs=epc_df, all_records=True, drop_epc_address=False
)

# %%
_merged = merged.drop(
    columns=[
        "mcs_address",
        "mcs_postcode",
        "mcs_numeric_tokens",
        "epc_postcode",
        "epc_numeric_tokens",
        "address_score",
    ]
)
assert mcs_epc_join_function.equals(_merged)

# %% [markdown]
# ## Prepare to take sample for address matching
#
# Here we will prepare to take a sample of 1002 matched addresses for reviewing the address matching.

# %%
mcs_epc = merged.copy()
mcs_epc = mcs_epc.rename(
    columns={
        "address_3": "mcs_address_3",
        "county": "mcs_county",
        "local_authority": "mcs_local_authority",
        "COUNTRY": "country",
    }
)

# %%
## Remove rows with no EPC match
## Any rows with all NaN values across all EPC columns in mcs_epc df will be removed
epc_cols = epc_df.columns.to_list()
epc_cols.extend(["epc_address", "epc_postcode", "epc_numeric_tokens"])

removed_epc_cols = [
    "ADDRESS1",
    "ADDRESS2",
    "POSTCODE",
    "COUNTRY",
    "standardised_postcode",
    "standardised_address",
    "numeric_tokens",
]

for removed in removed_epc_cols:
    epc_cols.remove(removed)

mcs_epc = mcs_epc.dropna(subset=epc_cols, how="all")

# %%
## In our sample, address matching only needs to be assessed on addresses where there is not an exact match
## First strip whitespace from comparing columns - this has been mistakenly added to some addresses in preprocessing
comparing_cols = ["mcs_address", "mcs_postcode", "epc_address", "epc_postcode"]

for col in comparing_cols:
    mcs_epc[col] = mcs_epc[col].str.strip()

# %%
## Now identify which matches occurred between non-identical addresses
## Our sample will take from this pool only because it shows us how effective the fuzzy matching is

mcs_epc["exact_match"] = mcs_epc.apply(
    lambda row: 1
    if (
        row["mcs_address"] == row["epc_address"]
        and row["mcs_postcode"] == row["epc_postcode"]
    )
    else 0,
    axis=1,
)
mcs_epc_non_exact = mcs_epc[mcs_epc["exact_match"] != 1]

# %% [markdown]
# ## Select a sample from the dataset to calculate error rate

# %%
## We will take 334 rows from each country to create a sample of 1002 address matches
country_sample_size = 334
seed = 7

# %%
countries = list(mcs_epc["country"].unique())

# %%
country_dfs = []

for country in countries:
    _df = mcs_epc_non_exact.loc[mcs_epc_non_exact["country"] == country]
    _sample = _df.sample(country_sample_size, random_state=seed)
    country_dfs.append(_sample)

# %%
mcs_epc_sample = pd.concat(country_dfs)

# %%
mcs_epc_sample.shape

# %%
## Keep relevant cols
mcs_epc_sample = mcs_epc_sample[
    [
        "mcs_address",
        "mcs_postcode",
        "epc_address",
        "epc_postcode",
        "mcs_numeric_tokens",
        "epc_numeric_tokens",
        "mcs_address_3",
        "mcs_county",
        "mcs_local_authority",
        "country",
        "exact_match",
        "address_score",
    ]
]

# %%
## Add col where results of manual review will be stored
mcs_epc_sample["confident_match"] = "check"

# %%
## Save dfs
date = datetime.today().strftime("%Y%m%d")

mcs_epc.to_csv(
    os.path.join(
        outputs_data_dir, f"{date}_mcs_epc_address_join_23Q2_prepr_dedupl_231009.csv"
    )
)
mcs_epc_sample.to_csv(
    os.path.join(
        outputs_data_dir,
        f"{date}_mcs_epc_address_matching_sample_n{country_sample_size*3}_seed{seed}.csv",
    )
)
mcs_epc_sample.to_excel(
    os.path.join(
        outputs_data_dir,
        f"{date}_mcs_epc_address_matching_sample_n{country_sample_size*3}_seed{seed}.xlsx",
    )
)

# %%
