# File: asf_core_data/pipeline/preprocessing/data_cleaning.py
"""Cleaning and standardising the EPC dataset.


DATA CLEANING GUIDELINES
------------------------

If you would like to clean an additional feature from the EPC or MCS dataset,
please stick to the following guidelines.

---
A) Simple mapping from a set of values to a standardised set of values

If you want to standardise values using a dict, add a respective dict entry in the
feature_cleaning_dict in data_cleaning_utils.py and add the feature name to the list
features_to_standardise.

Note that all values not found in the dict will be mapped to np.nan/unknown.

For example:

...
{"FEATURE_TO_CLEAN"  :  {'undesired value 1":"desired value A",
                         'undesired value 2":"desired value A",
                         'undesired value 3":"desired value B"}
}

features_to_standardise = [...,..., "FEATURE_TO_CLEAN"]

---
B) Custom function for standardising and cleaning features

If a simple dict mapping is not sufficient, e.g. because you need to apply regular expressions,
create a custom function in data_cleaning.py and name it clean_FEATURE_TO_CLEAN().

The function can either take B1) a single value or B2) the entire dataframe as input.

Examples for B1) cases are clean_POSTCODE() and clean_FLOOR_LEVEL().
Examples for B2) cases are clean_PHOTO_SUPPLY() and clean_EFF_SCORES().

Make sure to add B1 cases to the custom_cleaning_dict in custom_clean_features().
Make sure to add B2 cases to custom_clean_features() above the line  # [Additional cleaning functions here].
Note that the function must contain a check whether the feature is even present in the given dataframe.

---
C) Add values that should be standardised to np.nan or "unknown"

Add the values to the list 'invalid_values' in data_cleaning_utils.py

---
D) Set a max value for features

Add the feature to the dict in custom_clean_features(). For example:

cap_value_dict = {
    "NUMBER_HABITABLE_ROOMS": 10,
    "NUMBER_HEATED_ROOMS": 10,
    "FEATURE_TO_CLEAN": 50
}


E) Any other way of cleaning data

Make sure it doesn't disrupt the current pipeline and feel free to add to these guidelines here.

"""
# ---------------------------------------------------------------------------------


import re

import pandas as pd
import numpy as np


from asf_core_data import PROJECT_DIR, get_yaml_config, Path
from asf_core_data.config import base_config
from asf_core_data.pipeline.preprocessing import data_cleaning_utils

# ---------------------------------------------------------------------------------

# Load config file
config = get_yaml_config(Path(str(PROJECT_DIR) + "/asf_core_data/config/base.yaml"))


def clean_POSTCODE(postcode, level="unit", with_space=True):
    """Get POSTCODE as unit, district, sector or area.

    Args:
        postcode (str):  Raw postcode
        level (str, optional): Desired postcode level.
            Options: district, area, sector, unit. Defaults to "unit".
        with_space (bool, optional): Whether to add space after district. Defaults to True.

    Returns:
        str: Specific postcode level.
    """

    postcode = postcode.strip()
    postcode = postcode.upper()

    seperation = " " if with_space else ""

    if level == "area":
        return re.findall(r"([A-Z]+)", postcode)[0]

    else:
        part_1 = postcode[:-3].strip()
        part_2 = postcode[-3:].strip()

        if level == "district":
            return part_1
        elif level == "sector":
            return part_1 + seperation + part_2[0]
        elif level == "unit":
            return part_1 + seperation + part_2
        else:
            raise IOError(
                "Postcode level '{}' unknown. Please select 'area', 'district', 'sector' or 'unit'.".format(
                    level
                )
            )


def clean_FLOOR_LEVEL(floor_level, as_numeric=base_config.FLOOR_LEVEL_AS_NUM):
    """Standardise floor level.
    By default, the floor level is given as a numeric value (between -1 and 10).
    Alternatively, the floor level can be returned as one of the following categories:
    -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10+

    Args:
        floor_level (str):  Floor level.
        as_numeric (bool, optional): Whehter to return floor level as numeric value. Defaults to base_config.FLOOR_LEVEL_AS_NUM.
            If set to False, return value as category.

    Returns:
        int/str: Standardised floor level
    """

    # floor_level = floor_level.strip()
    floor_level_dict = data_cleaning_utils.feature_cleaning_dict["FLOOR_LEVEL"]

    if floor_level in floor_level_dict.keys():
        if as_numeric:
            return floor_level_dict[floor_level]
        else:
            category = str(floor_level_dict[floor_level])
            category = "10+" if category == "10" else category
            return category
    else:
        standard_floor_level = np.nan if as_numeric else "unknown"
        return standard_floor_level


def clean_CONSTRUCTION_AGE_BAND(age_band, merged_bands=base_config.MERGED_AGE_BANDS):
    """Standardise construction age bands and if necessary adjust
    the age bands to combine the Scotland and England/Wales data.

    Args:
        age_band (str): Raw construction age.
        merged_bands (bool, optional):  Whether to merge Scotland and England/Wales age bands. Defaults to base_config.MERGED_AGE_BANDS.

    Returns:
        str: Standardised age construction band.
    """

    age_band = age_band.strip()

    if merged_bands:
        age_band_dict = data_cleaning_utils.feature_cleaning_dict[
            "CONSTRUCTION_AGE_BAND_MERGED"
        ]
    else:
        age_band_dict = data_cleaning_utils.feature_cleaning_dict[
            "CONSTRUCTION_AGE_BAND"
        ]

    if age_band in age_band_dict.keys():
        return age_band_dict[age_band]
    else:
        return "unknown"


def clean_LOCAL_AUTHORITY(local_authority):
    """Clean local authority label.

    Args:
        local_authority (str): Local authority label.

    Returns:
        str:  Cleaned local authority.
    """

    if local_authority in ["00EM", "16UD"]:
        return "unknown"
    else:
        return local_authority


def clean_GLAZED_AREA(area, as_numeric=base_config.GLAZED_AREA_AS_NUM):
    """Standardise glazed area type.
    By default, the glazed area is given as a numeric value (between 1 and 5).
    Alternatively, the glazed area can be returned as one of the following categories:
    Normal, More Than Typical, Much More Than Typical, Less Than Typical, Much Less Than Typical, unknown.

    Args:
        area (str): Glazed area type.
        as_numeric (bool, optional):  Return glazed area as numeric value.
            If set to False, return value as category.
            Defaults to base_config.GLAZED_AREA_AS_NUM.

    Returns:
        str: Standardised glazed area.
    """

    glazed_area_cat = data_cleaning_utils.feature_cleaning_dict["GLAZED_AREA_CAT"]
    glazed_area_num = data_cleaning_utils.feature_cleaning_dict["GLAZED_AREA_NUM"]

    # Standardise (numeric or cat) using dict
    if area in glazed_area_num.keys():
        if as_numeric:
            return glazed_area_num[area]
        else:
            return glazed_area_cat[glazed_area_num[area]]

    # Handle unknown/NaN
    else:
        standardised_area = np.nan if as_numeric else "unknown"
        return standardised_area


def clean_PHOTO_SUPPLY(df):
    """Extract photo supply area from lengthy descriptions.

    Args:
        df (pandas.DataFrame): Dataframe to modify.

    Returns:
        pd.DateFrame: Dataframe with fixed photo supply column.
    """

    if "PHOTO_SUPPLY" not in df.columns:
        return df

    # Set all values mentioning "Peak Power" to "unknown" because they do not contain photo supply area
    df["PHOTO_SUPPLY"] = df["PHOTO_SUPPLY"].str.replace(
        r".*Peak Power.*", "unknown", regex=True
    )

    # Extract photo supply area from value
    df["PHOTO_SUPPLY"] = df["PHOTO_SUPPLY"].str.extract(r"(\d+)[\%|\.d$]").astype(float)
    return df


def create_efficiency_mapping(efficiency_set, only_first=base_config.ONLY_FIRST_EFF):
    """Create dict to map efficiency label(s) to numeric value.

    Args:
        efficiency_set (list): List of efficiencies as encoded as strings.
        only_first (bool, optional): Only consider first efficiency entry if where are multiple.
            Defaults to base_config.ONLY_FIRST_EFF.

    Returns:
        dict: Map efficiency labels to numeric values.
    """

    efficiency_map = {}

    for eff in efficiency_set:

        # If efficiency is float (incl. NaN)
        if isinstance(eff, float):
            efficiency_map[eff] = np.nan
            continue

        # Split parts of label (especially for Scotland data)
        eff_parts = [
            part.strip()
            for part in eff.split("|")
            if part.strip() not in ["n/a", "unknown", ""]
        ]

        if not eff_parts:
            efficiency_map[eff] = np.nan
            continue

        if only_first:
            eff_parts = eff_parts[:1]

        # Map labels to numeric value and take mean
        eff_value = sum(
            [data_cleaning_utils.eff_value_dict[part] for part in eff_parts]
        ) / float(len(eff_parts))

        efficiency_map[eff] = round(eff_value, 1)

    return efficiency_map


def clean_EFF_SCORES(df):
    """Clean and modify energy efficiency and environment ratings and scores for different categories,
    e.g. WINDOWS_ENVIRONMENT_EFF.

    Standardise ratings and get average value if several values given.
    Capture both as categorical feature (very poor to very good) and as numeric features (1.0 to 5.0), ending with "_SCORE".

    Args:
        df (pandas.DataFrame): Dataframe to modify.

    Returns:
        pandas.DataFrame: Dataframe with updated efficiency score features.
    """

    for feat in df.columns:
        # If efficiency feature, get respective mapping
        if feat.endswith("_EFF"):

            df[feat] = df[feat].str.lower()
            map_dict = create_efficiency_mapping(list(df[feat].unique()))

            # Encode features
            df[feat + "_SCORE"] = df[feat].map(map_dict)
            df[feat] = round(df[feat + "_SCORE"]).map(
                data_cleaning_utils.value_eff_dict
            )

    return df


def cap_feature_values(df, feature, cap_value=10, as_cat=False):
    """Cap the values of a given feature, in order to reduce the
    effect of outliers.
    For example, set NUMBER_OF_HABITABLE_ROOMS values that are
    greater/equal to 10 to "10+".


    Args:
        df (pandas.DataFrame): Dataframe to modify.
        feature (str):  Feature for which values are capped.
        cap_value (int, optional): Max value / where to cap. Defaults to 10.
        as_cat (bool, optional): Save as category instead of numeric feature. Defaults to False.

    Returns:
        pandas.DataFrame: Dataframe with capped values.
    """

    # Cap at given limit (i.e. 10)
    cap_n = str(cap_n) + "+" if as_cat else cap_value
    df.loc[(df[feature] >= cap_n), feature] = cap_value

    if as_cat:
        df[feature] = df[feature].astype(str)

    return df


def standardise_dates(
    df, date_features=["INSPECTION_DATE", "LODGEMENT_DATE", "LODGEMENT_DATETIME"]
):
    """Standardise date features and transform to datetime values for easier handling.
    Test for unreasonable dates and fix years starting with 00.

    Args:
        df (pandas.DataFrame): Dataframe to modify.
        date_features (list, optional): Date features to modify. Defaults to ["INSPECTION_DATE", "LODGEMENT_DATE", "LODGEMENT_DATETIME"].

    Returns:
        pandas.DataFrame: Dataframe with standardises date features.
    """

    date_features = [
        date_feat for date_feat in date_features if date_feat in df.columns
    ]

    for feature in date_features:

        # Fix years starting with 00 -> 20..
        df[feature] = (
            df[feature].astype(str).str.replace(r"00(\d\d)", r"20\1", regex=True)
        )
        df[feature] = pd.to_datetime(df[feature], errors="coerce")

        df.loc[
            (df[feature].dt.year > base_config.CURRENT_YEAR + 1)
            | (df[feature].dt.year < 2008),
            feature,
        ] = np.datetime64("NaT")

    return df


def standardise_unknowns(df):
    """Standardise unknown and invalid values.
    For numeric features, change invalid values to NaN.
    For categorical features, change invalid values to "unknown".

    Args:
        df (pandas.DataFrame): Dataframe to modify.

    Returns:
        pandas.DataFrame: Dataframe with cleaned up unknown values.
    """
    for feat in df.columns:

        if feat in data_cleaning_utils.numeric_features:
            df[feat] = df[feat].replace(data_cleaning_utils.invalid_values, np.nan)
        else:
            df[feat] = df[feat].replace(data_cleaning_utils.invalid_values, "unknown")

    return df


def standardise_features(df):
    """Standardise features using a mapping dict.

    Args:
        df (pandas.DataFrame): Dataframe to modify.

    Returns:
        pandas.DataFrame: Dataframe with stanardised features.
    """

    for feat in df.columns:

        if feat in data_cleaning_utils.features_to_standardise:

            df[feat] = df[feat].str.strip()
            feat_clean_dict = data_cleaning_utils.feature_cleaning_dict[feat]

            df[feat] = df[feat].str.strip()
            df[feat] = df[feat].map(feat_clean_dict)
            df[feat].fillna("unknown", inplace=True)

    return df


def remove_empty_features(df):
    """Remove empty features, i.e. features/columns that only have one unique value.

    Args:
        df (pandas.DataFrame): Dataframe to modify.

    Returns:
        pandas.DataFrame: Dataframe with removed empty features.
    """
    # Remove all columns with only NaN
    df.dropna(axis=1, how="all", inplace=True)

    # Remove all columns that have only same value
    nunique = df.nunique()
    cols_to_drop = nunique[nunique == 1].index
    df.drop(cols_to_drop, axis=1, inplace=True)

    return df


def custom_clean_features(df):
    """Custom clean features.
    For instances, standardise values and cap at max value.

    Args:
        df (pandas.DataFrame): Dataframe to modify.

    Returns:
        pandas.DataFrame: Dataframe after custom cleaning features.
    """

    custom_cleaning_dict = {
        "POSTCODE": clean_POSTCODE,
        "CONSTRUCTION_AGE_BAND": clean_CONSTRUCTION_AGE_BAND,
        "LOCAL_AUTHORITY_LABEL": clean_LOCAL_AUTHORITY,
        "FLOOR_LEVEL": clean_FLOOR_LEVEL,
        "GLAZED_AREA": clean_GLAZED_AREA,
    }

    cap_value_dict = {
        "NUMBER_HABITABLE_ROOMS": 10,
        "NUMBER_HEATED_ROOMS": 10,
    }

    # Custom cleaning by value (B1)
    for feat in df.columns:
        if feat in custom_cleaning_dict.keys():
            df[feat] = df[feat].apply(custom_cleaning_dict[feat])

    # Custom cleaning by df (B2) - usually categorical to numeric feature cleaning
    df = clean_PHOTO_SUPPLY(df)
    df = clean_EFF_SCORES(df)
    # [Additional cleaning functions here]

    # Cap features
    for feat in df.columns:
        if feat in cap_value_dict.keys():
            cap_value = cap_value_dict[feat]
            df = cap_feature_values(df, feat, cap_value=cap_value)

    return df


def clean_epc_data(df):
    """Standardise and clean EPC data.
    For example, reformat dates and standardise categories.

    Args:
        df (pandas.DataFrame): Raw/original EPC dataframe.

    Returns:
        pandas.DataFram: Standarised and cleaned EPC dataframe.
    """

    df = remove_empty_features(df)

    df = standardise_unknowns(df)
    df = standardise_features(df)
    df = standardise_dates(df)
    df = custom_clean_features(df)

    return df


def reformat_postcode(df):
    """Reformat postcode (remove empty space).
    Legacy code for backwards-compatibility.

    Args:
        df (pd.DataFrame): Dataframe with postcode column.

    Returns:
        df (pd.DataFrame): Updated dataframe with formatted postcode.
    """

    df["POSTCODE"] = df["POSTCODE"].str.replace(r" ", "")

    return df
