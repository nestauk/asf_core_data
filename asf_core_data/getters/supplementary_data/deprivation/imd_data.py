# File: getters/deprivation_data.py
"""Loading deprivation data for differnet countries."""

# ---------------------------------------------------------------------------------

from asf_core_data import PROJECT_DIR, get_yaml_config, Path
import pandas as pd
from asf_core_data.config import base_config

# ---------------------------------------------------------------------------------

# Load config file
config = get_yaml_config(
    Path(str(PROJECT_DIR) + "/heat_pump_adoption_modelling/config/base.yaml")
)

country_path_dict = {
    "England": base_config.IMD_ENGLAND_PATH,
    "Wales": base_config.IMD_WALES_PATH,
    "Scotland": base_config.IMD_SCOTLAND_PATH,
}


def get_country_imd_data(country, data_path=PROJECT_DIR, usecols=None):
    """Get deprivation data for specific country.

    Args:
        country (str): Country for which to load IMD: 'England', 'Scotland', 'Wales'.
        data_path (str/Path, optional): Path to ASF core data directory or 'S3'. Defaults to PROJECT_DIR.
        usecols (list, optional): List of features/columns to load. Defaults to None, loading all features.

    Returns:
        pandas.DataFrame: Deprivation data.
    """

    file_path = data_path / country_path_dict[country]

    imd_df = pd.read_csv(file_path, usecols=usecols)
    imd_df["Country"] = country

    return imd_df


def get_gb_imd_data(
    usecols=[
        "IMD Rank",
        "IMD Decile",
        "Postcode",
        "Income Score",
        "Employment Score",
        "Country",
    ]
):
    """Get deprivation data for England, Wales and Scotland.

    Args:
        usecols (list, optional):  List of features/columns to load.
            Defaults to ["IMD Rank", "IMD Decile", "Postcode", "Income Score", "Employment Score", "Country"].
            This selection works for all countries. None will load all features.

    Returns:
        pandas.DataFrame:  Deprivation data for all countries.
    """

    england_imd = get_country_imd_data("England", usecols=usecols)
    wales_imd = get_country_imd_data("Wales", usecols=usecols)
    scotland_imd = get_country_imd_data("Scotland", usecols=usecols)

    imd_df = pd.concat([england_imd, wales_imd, scotland_imd], axis=0)

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

    merged_df = pd.merge(other_df, imd_df, on=["Postcode"])

    merged_df = merged_df.rename(columns={"Postcode": postcode_label})

    return merged_df
