# File: getters/deprivation_data.py
"""Loading deprivation data for differnet countries."""

# ---------------------------------------------------------------------------------

import pandas as pd
from asf_core_data.config import base_config

from asf_core_data.getters import data_getters

# ---------------------------------------------------------------------------------

# Load config file

country_path_dict = {
    "england": base_config.IMD_ENGLAND_PATH,
    "wales": base_config.IMD_WALES_PATH,
    "scotland": base_config.IMD_SCOTLAND_PATH,
}


def get_imd_data(
    country="GB",
    data_path="S3",
    rel_path=None,
    usecols=[
        "IMD Rank",
        "IMD Decile",
        "Postcode",
        "Income Score",
        "Employment Score",
        "Country",
    ],
):
    """Get deprivation data for specific country.

    Args:
        country (str, optional): Country for which to load IMD: 'England', 'Scotland', 'Wales' or 'GB'. Defaults to 'GB'.
        data_path (str, optional): Path to IMD data in local dir or 'S3'. Defaults to "S3".
        rel_path (str/Path, optional): Relative path to IMD data. Defaults to None, leading to loading from default location.
        usecols (list, optional): List of features/columns to load. Defaults to ["IMD Rank", "IMD Decile", "Postcode", "Income Score", "Employment Score", "Country"].

    Raises:
        IOError: Throws error if country input is not valid.

    Returns:
        pandas.DataFrame: Deprivation data.
    """

    country = country.lower()
    country_name = country[0].upper() + country[1:] if country != "gb" else "GB"

    if country == "gb":
        england_imd = get_imd_data(
            "England",
            data_path=data_path,
            usecols=usecols,
        )

        wales_imd = get_imd_data(
            "Wales",
            data_path=data_path,
            usecols=usecols,
        )

        scotland_imd = get_imd_data(
            "Scotland",
            data_path=data_path,
            usecols=usecols,
        )

        imd_df = pd.concat([england_imd, wales_imd, scotland_imd], axis=0)

    elif country in ["england", "wales", "scotland"]:
        if rel_path is None:
            rel_path = country_path_dict[country]

        imd_df = data_getters.load_data(rel_path, data_path=data_path, usecols=usecols)
        imd_df["Country"] = country_name

    else:
        raise IOError(
            "'{}' is not a valid country/area. Please pick from 'England', 'Wales', 'Scotland' or 'GB'.".format(
                country
            )
        )

    return imd_df


def merge_imd_with_other_set(imd_df, other_df, postcode_label="Postcode"):
    """Merge IMD data with other data based on postcode.

    Args:
        imd_df (pandas.DataFrame): Deprivation data.
        other_df (pandas.DataFrame): Other data.
        postcode_label (str, optional):  How to rename postcode label. Defaults to "Postcode".

    Returns:
        pandas.DataFrame:  Two datasets merged on postcode.
    """

    if "POSTCODE" in other_df.columns:
        other_df = other_df.rename(columns={"POSTCODE": "Postcode"})

    imd_df["Postcode"] = imd_df["Postcode"].str.replace(r" ", "")
    other_df["Postcode"] = other_df["Postcode"].str.replace(r" ", "")

    merged_df = pd.merge(other_df, imd_df, on=["Postcode"], how="left")

    merged_df = merged_df.rename(columns={"Postcode": postcode_label})

    return merged_df
