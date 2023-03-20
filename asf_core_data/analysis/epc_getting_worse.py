import sys

sys.path.insert(0, "/Users/juliasuter/Documents/repositories/asf_core_data")

from asf_core_data.getters.epc import epc_data

LOCAL_DATA_DIR = "/Users/juliasuter/Documents/ASF_data"


epc_mapping = {"A": 6, "B": 5, "C": 4, "D": 3, "E": 2, "F": 1, "G": 0}


# Loading
epc_df = epc_data.load_preprocessed_epc_data(
    data_path=LOCAL_DATA_DIR,
    version="preprocessed",
    usecols=[
        "UPRN",
        "COUNTRY",
        "CURRENT_ENERGY_RATING",
        "CURRENT_ENERGY_EFFICIENCY",
        "ENERGY_RATING_CAT",
        "HP_INSTALLED",
        "N_SAME_UPRN_ENTRIES",
        "INSPECTION_DATE",
    ],
    batch="2022_Q3_complete",
)

print("Number of samples:", epc_df.shape[0])


# Adding column for EPC rating score
epc_df["EPC_RATING_AS_SCORE"] = epc_df["CURRENT_ENERGY_RATING"].map(epc_mapping)


# Reducing to relevant samples
mult_epcs = epc_df[epc_df["N_SAME_UPRN_ENTRIES"] > 1]

print("Number of properties:", len(mult_epcs["UPRN"].unique()))

first = (
    mult_epcs.sort_values("INSPECTION_DATE", ascending=True).drop_duplicates(
        subset=["UPRN"], keep="first"
    )
).sort_index()


last = (
    mult_epcs.sort_values("INSPECTION_DATE", ascending=False).drop_duplicates(
        subset=["UPRN"], keep="first"
    )
).sort_index()


assert len(mult_epcs["UPRN"].unique()) == first.shape[0]
assert len(mult_epcs["UPRN"].unique()) == last.shape[0]

first.rename(
    columns={
        "CURRENT_ENERGY_RATING": "CURRENT_ENERGY_RATING_AT_FIRST",
        "HP_INSTALLED": "HP_INSTALLED_AT_FIRST",
        "CURRENT_ENERGY_EFFICIENCY": "CURRENT_ENERGY_EFFICIENCY_AT_FIRST",
        "ENERGY_RATING_CAT": "ENERGY_RATING_CAT_AT_FIRST",
        "EPC_RATING_AS_SCORE": "EPC_RATING_AS_SCORE_AT_FIRST",
    },
    inplace=True,
)

first_last_epcs = first.merge(
    last[
        [
            "UPRN",
            "INSPECTION_DATE",
            "CURRENT_ENERGY_RATING",
            "ENERGY_RATING_CAT",
            "HP_INSTALLED",
            "EPC_RATING_AS_SCORE",
            "CURRENT_ENERGY_EFFICIENCY",
        ]
    ],
    on=["UPRN"],
)

# EPCs with no HP at first but later on
hp_added_epcs = first_last_epcs[
    ~first_last_epcs["HP_INSTALLED_AT_FIRST"] & first_last_epcs["HP_INSTALLED"]
]

hp_added_epcs["EPC_CAT_DIFF"] = (
    hp_added_epcs["EPC_RATING_AS_SCORE"] - hp_added_epcs["EPC_RATING_AS_SCORE_AT_FIRST"]
)
cat_diff = (
    round(
        hp_added_epcs[hp_added_epcs["EPC_CAT_DIFF"] < 0].shape[0]
        / hp_added_epcs.shape[0],
        4,
    )
    * 100
)

hp_added_epcs["EPC_SCORE_DIFF"] = (
    hp_added_epcs["CURRENT_ENERGY_EFFICIENCY"]
    - hp_added_epcs["CURRENT_ENERGY_EFFICIENCY_AT_FIRST"]
)
score_diff = (
    round(
        hp_added_epcs[hp_added_epcs["EPC_SCORE_DIFF"] < 0].shape[0]
        / hp_added_epcs.shape[0],
        4,
    )
    * 100
)

print("Differences in EPC category: {}%".format(cat_diff))
print("Differences in EPC score: {}%".format(score_diff))
