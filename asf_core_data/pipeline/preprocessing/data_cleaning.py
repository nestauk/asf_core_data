# File: heat_pump_adoption_modelling/pipeline/preprocessing/data_cleaning.py
"""Cleaning and standardising the EPC dataset."""


import re

from asf_core_data import PROJECT_DIR, get_yaml_config, Path
from asf_core_data.config import base_config
from numpy.core import numeric
import pandas as pd
import numpy as np

from asf_core_data.pipeline.preprocessing import data_cleaning_utils


# Load config file
config = get_yaml_config(Path(str(PROJECT_DIR) + "/asf_core_data/config/base.yaml"))


def clean_POSTCODE(postcode, level="unit", with_space=True):
    """Get POSTCODE as unit, district, sector or area.

    Args:
        postcode (str): Raw postcode
        level (str): Desired postcode level, defaults to "unit".
            Options: district, area, sector, unit
        with_space (bool): Whether to add space after district, defaults to True.

    Returns:
        level: Specific postcode level.
    """

    """Get POSTCODE as unit, district, sector or area.

    Parameters:
    ----------
    postcode : str
        Raw postcode.

    level : str, {'district', 'area', 'sector', 'unit'}, optional
        Desired postcode level, defaults to 'unit'.

    with_space : bool, optional
        Whether to add space after district, defaults to True.

    Returns
    -------
        level: str
            Specific postcode level.
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

    Parameters
    ----------
    floor_level : str
        Floor level.

    as_numeric : bool, default=True
        Return floor level as numeric value.
        If set to False, return value as category.

    Return
    ----------
    standardised floor level : int, or str
        Standardised floor level."""

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

    Parameters
    ----------
    age : str
        Raw construction age.

    merge_country_data : bool, default=True
        Whether to merge Scotland and England/Wales age bands.

    Return
    ----------
    Standardised age construction band : str
        Standardised age construction band."""

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

    Paramters
    ----------
    local_authority : str
        Local authority label.

    Return
    ----------
    local_authority : str
        Cleaned local authority."""

    if local_authority in ["00EM", "16UD"]:
        return "unknown"
    else:
        return local_authority


def clean_GLAZED_AREA(area, as_numeric=base_config.GLAZED_AREA_AS_NUM):
    """Standardise glazed area type.
    By default, the glazed area is given as a numeric value (between 1 and 5).
    Alternatively, the glazed area can be returned as one of the following categories:
    Normal, More Than Typical, Much More Than Typical, Less Than Typical, Much Less Than Typical, unknown.

    Parameters
    ----------
    area : str
        Glazed area type.

    as_numeric : bool, default=True
        Return glazed area as numeric value.
        If set to False, return value as category.

    Return
    ----------
    standardised glazed area : int, or str
        Standardised glazed area."""

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
    """Extract photo supply area from length descriptions.

    Paramters
    ----------
    df : pandas.DataFrame
        Given dataframe.

    Return
    ---------
     df : pandas.DataFrame
        Dataframe with fixed photo supply column."""

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

    Parameters
    ----------
    efficiency_set : list
        List of efficiencies as encoded as strings.

    Return
    ---------
    efficiency_map : dict
        Dict to map efficiency labels to numeric values."""

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


def cap_feature_values(df, feature, cap_n=10, as_cat=False):
    """Cap the values of a given feature, in order to reduce the
    effect of outliers.
    For example, set NUMBER_OF_HABITABLE_ROOMS values that are
    greater/equal to 10 to "10+".

    Paramters
    ----------
    df : pandas.DataFrame
        Given dataframe.

    feature : str
        Feature for which values are capped.

    cap_n : int, default=10
        At which value to cap.

    Return
    ---------
     df : pandas.DataFrame
        Dataframe with capped values."""

    # Cap at given limit (i.e. 10)
    cap_n = str(cap_n) + "+" if as_cat else cap_n
    df.loc[(df[feature] >= cap_n), feature] = cap_n

    if as_cat:
        df[feature] = df[feature].astype(str)

    return df


def standardise_dates(
    df, date_features=["INSPECTION_DATE", "LODGEMENT_DATE", "LODGEMENT_DATETIME"]
):

    for feature in date_features:

        df[feature] = df[feature].str.replace(r"00(\d\d)", r"20\1", regex=True)

        df[feature] = pd.to_datetime(df[feature], errors="coerce")

        df.loc[
            (df[feature].dt.year > base_config.CURRENT_YEAR + 1)
            | (df[feature].dt.year < 2008),
            feature,
        ] = np.datetime64("NaT")

    return df


def standardise_unknowns(df):

    for feat in df.columns:

        if feat in data_cleaning_utils.numeric_features:
            df[feat] = df[feat].replace(data_cleaning_utils.invalid_values, np.nan)
        else:
            df[feat] = df[feat].replace(data_cleaning_utils.invalid_values, "unknown")

    return df


def standardise_features(df):

    for feat in df.columns:

        if feat in [
            "TENURE",
            "SOLAR_WATER_HEATING_FLAG",
            "TRANSACTION_TYPE",
            "GLAZED_TYPE",
            "ENERGY_TARIFF",
        ]:

            df[feat] = df[feat].str.strip()
            feat_clean_dict = data_cleaning_utils.feature_cleaning_dict[feat]

            df[feat] = df[feat].str.strip()
            df[feat] = df[feat].map(feat_clean_dict)
            df[feat].fillna("unknown", inplace=True)

    return df


def remove_empty_features(df):

    # Remove all columns with only NaN
    df.dropna(axis=1, how="all", inplace=True)

    # Remove all columns that have only same value
    nunique = df.nunique()
    cols_to_drop = nunique[nunique == 1].index
    df.drop(cols_to_drop, axis=1, inplace=True)

    return df


def clean_epc_data(df):
    """Standardise and clean EPC data.
    For example, reformat dates and standardise categories.

    Parameters
    ----------
    df : pandas.DataFrame
        Raw/original EPC dataframe.

    Return
    ----------
    df : pandas.DataFrame
        Standarised and cleaned EPC dataframe."""

    column_to_function_dict = {
        "POSTCODE": clean_POSTCODE,
        "CONSTRUCTION_AGE_BAND": clean_CONSTRUCTION_AGE_BAND,
        "LOCAL_AUTHORITY_LABEL": clean_LOCAL_AUTHORITY,
        "FLOOR_LEVEL": clean_FLOOR_LEVEL,
        "GLAZED_AREA": clean_GLAZED_AREA,
    }

    df = remove_empty_features(df)
    df = standardise_unknowns(df)
    df = standardise_features(df)
    df = standardise_dates(df)

    for feat in df.columns:
        if feat in column_to_function_dict.keys():
            print(feat)
            df[feat] = df[feat].apply(column_to_function_dict[feat])

    df = clean_PHOTO_SUPPLY(df)
    df = clean_EFF_SCORES(df)

    if "NUMBER_HABITABLE_ROOMS" in df.columns:
        # Limit max value for NUMBER_HABITABLE_ROOMS
        df = cap_feature_values(df, "NUMBER_HABITABLE_ROOMS", cap_n=10)

    return df


# def standardise_efficiency(efficiency):
#     """Standardise efficiency types; one of the five categories:
#     poor, very poor, average, good, very good

#     Parameters
#     ----------
#     efficiency : str
#         Raw efficiency type.

#     Return
#     ----------
#     standardised efficiency : str
#         Standardised efficiency type."""

#     # Handle NaN
#     if isinstance(efficiency, float):
#         return "unknown"

#     efficiency = efficiency.lower().strip()
#     efficiency = efficiency.strip('"')
#     efficiency = efficiency.strip()
#     efficiency = efficiency.strip("|")
#     efficiency = efficiency.strip()

#     efficiency_mapping = {
#         "poor |": "Poor",
#         "very poor |": "Very Poor",
#         "average |": "Average",
#         "good |": "Good",
#         "very good |": "Very Good",
#         "poor": "Poor",
#         "very poor": "Very Poor",
#         "average": "Average",
#         "good": "Good",
#         "very good": "Very Good",
#         "n/a": "unknown",
#         "n/a |": "unknown",
#         "n/a": "unknown",
#         "n/a | n/a": "unknown",
#         "n/a | n/a | n/a": "unknown",
#         "n/a | n/a | n/a | n/a": "unknown",
#         "no data!": "unknown",
#         "unknown": "unknown",
#     }

#     return efficiency_mapping[efficiency


# def standardise_tenure(tenure):
#     """Standardise tenure types; one of the four categories:
#     rental (social), rental (private), owner-occupied, unknown

#     Parameters
#     ----------
#     tenure : str
#         Raw tenure type.

#     Return
#     ----------
#     standardised tenure : str
#         Standardised tenure type."""

#     # Catch NaN
#     if isinstance(tenure, float):
#         return "unknown"

#     tenure = tenure.lower()
#     tenure_mapping = {
#         "owner-occupied": "owner-occupied",
#         "rental (social)": "rental (social)",
#         "rented (social)": "rental (social)",
#         "rental (private)": "rental (private)",
#         "rented (private)": "rental (private)",
#         "unknown": "unknown",
#         "no data!": "unknown",
#         "not defined - use in the case of a new dwelling for which the intended tenure in not known. it is no": "unknown",
#     }

#     return tenure_mapping[tenure]


# def standardise_solar_water_heating_flag(flag):

#     flag_dict = {
#         "N": "False",
#         "unknown": "unknown",
#         "Y": "True",
#         "false": "False",
#         "true": "True",
#     }

#     if flag not in flag_dict.keys():
#         return "unknown"
#     else:
#         return flag_dict[flag]


# def standardise_tenure(tenure):
#     """Standardise tenure types; one of the four categories:
#     rental (social), rental (private), owner-occupied, unknown

#     Parameters
#     ----------
#     tenure : str
#         Raw tenure type.

#     Return
#     ----------
#     standardised tenure : str
#         Standardised tenure type."""

#     # Catch NaN
#     if isinstance(tenure, float):
#         return "unknown"

#     tenure = tenure.lower()
#     tenure_mapping = {
#         "owner-occupied": "owner-occupied",
#         "rental (social)": "rental (social)",
#         "rented (social)": "rental (social)",
#         "rental (private)": "rental (private)",
#         "rented (private)": "rental (private)",
#         "unknown": "unknown",
#         "no data!": "unknown",
#         "not defined - use in the case of a new dwelling for which the intended tenure in not known. it is no": "unknown",
#     }

#     return tenure_mapping[tenure]


# def clean_local_authority(local_authority):
#     """Clean local authority label.

#     Paramters
#     ----------
#     local_authority : str
#         Local authority label.

#     Return
#     ----------
#     local_authority : str
#         Cleaned local authority."""

#     if local_authority in ["00EM", "16UD"]:
#         return "unknown"
#     else:
#         return local_authority


# def standardise_constr_age(age, adjust_age_bands=True):

#     """Standardise construction age bands and if necessary adjust
#     the age bands to combine the Scotland and England/Wales data.

#     Parameters
#     ----------
#     age : str
#         Raw construction age.

#     merge_country_data : bool, default=True
#         Whether to merge Scotland and England/Wales age bands.

#     Return
#     ----------
#     Standardised age construction band : str
#         Standardised age construction band."""

#     # Handle NaN
#     if isinstance(age, float):
#         return "unknown"

#     age = age.strip()

#     age_mapping = {
#         "England and Wales: before 1900": "England and Wales: before 1900",
#         "England and Wales: 1900-1929": "England and Wales: 1900-1929",
#         "England and Wales: 1930-1949": "England and Wales: 1930-1949",
#         "England and Wales: 1950-1966": "England and Wales: 1950-1966",
#         "England and Wales: 1967-1975": "England and Wales: 1967-1975",
#         "England and Wales: 1976-1982": "England and Wales: 1976-1982",
#         "England and Wales: 1983-1990": "England and Wales: 1983-1990",
#         "England and Wales: 1991-1995": "England and Wales: 1991-1995",
#         "England and Wales: 1996-2002": "England and Wales: 1996-2002",
#         "England and Wales: 2003-2006": "England and Wales: 2003-2006",
#         "England and Wales: 2007-2011": "England and Wales: 2007-2011",
#         "England and Wales: 2007-2011": "England and Wales: 2007 onwards",
#         "England and Wales: 2007 onwards": "England and Wales: 2007 onwards",
#         "England and Wales: 2012 onwards": "England and Wales: 2012 onwards",
#         "1900": "England and Wales: 1900-1929",
#         "2021": "England and Wales: 2012 onwards",
#         "2020": "England and Wales: 2012 onwards",
#         "2019": "England and Wales: 2012 onwards",
#         "2018": "England and Wales: 2012 onwards",
#         "2017": "England and Wales: 2012 onwards",
#         "2016": "England and Wales: 2012 onwards",
#         "2015": "England and Wales: 2012 onwards",
#         "2014": "England and Wales: 2012 onwards",
#         "2013": "England and Wales: 2012 onwards",
#         "2012": "England and Wales: 2012 onwards",
#         "2011": "England and Wales: 2007 onwards",
#         "2010": "England and Wales: 2007 onwards",
#         "2009": "England and Wales: 2007 onwards",
#         "2008": "England and Wales: 2007 onwards",
#         "2007": "England and Wales: 2007 onwards",
#         "before 1919": "Scotland: before 1919",
#         "1919-1929": "Scotland: 1919-1929",
#         "1930-1949": "Scotland: 1930-1949",
#         "1950-1964": "Scotland: 1950-1964",
#         "1965-1975": "Scotland: 1965-1975",
#         "1976-1983": "Scotland: 1976-1983",
#         "1984-1991": "Scotland: 1984-1991",
#         "1992-1998": "Scotland: 1992-1998",
#         "1999-2002": "Scotland: 1999-2002",
#         "2003-2007": "Scotland: 2003-2007",
#         "2008 onwards": "Scotland: 2008 onwards",
#     }

#     age_mapping_adjust_age_bands = {
#         "England and Wales: before 1900": "England and Wales: before 1900",
#         "England and Wales: 1900-1929": "1900-1929",
#         "England and Wales: 1930-1949": "1930-1949",
#         "England and Wales: 1950-1966": "1950-1966",
#         "England and Wales: 1967-1975": "1965-1975",
#         "England and Wales: 1976-1982": "1976-1983",
#         "England and Wales: 1983-1990": "1983-1991",
#         "England and Wales: 1991-1995": "1991-1998",
#         "England and Wales: 1996-2002": "1996-2002",
#         "England and Wales: 2003-2006": "2003-2007",
#         "England and Wales: 2007-2011": "2007 onwards",
#         "England and Wales: 2007 onwards": "2007 onwards",
#         "England and Wales: 2012 onwards": "2007 onwards",
#         "2021": "2007 onwards",
#         "2020": "2007 onwards",
#         "2019": "2007 onwards",
#         "2018": "2007 onwards",
#         "2017": "2007 onwards",
#         "2016": "2007 onwards",
#         "2015": "2007 onwards",
#         "2015": "2007 onwards",
#         "2014": "2007 onwards",
#         "2013": "2007 onwards",
#         "2013": "2007 onwards",
#         "2012": "2007 onwards",
#         "2011": "2007 onwards",
#         "2010": "2007 onwards",
#         "2009": "2007 onwards",
#         "2008": "2007 onwards",
#         "2007": "2007 onwards",
#         "1900": "1900-1929",
#         "before 1919": "Scotland: before 1919",
#         "1919-1929": "1900-1929",
#         "1930-1949": "1930-1949",
#         "1950-1964": "1950-1966",
#         "1965-1975": "1965-1975",
#         "1976-1983": "1976-1983",
#         "1984-1991": "1983-1991",
#         "1992-1998": "1991-1998",
#         "1999-2002": "1996-2002",
#         "2003-2007": "2003-2007",
#         "2008 onwards": "2007 onwards",
#         "unknown": "unknown",
#         "NO DATA!": "unknown",
#         "INVALID!": "unknown",
#         "Not applicable": "unknown",
#     }

#     if adjust_age_bands:

#         if age not in age_mapping_adjust_age_bands:
#             return "unknown"
#         else:
#             return age_mapping_adjust_age_bands[age]
#     else:
#         if age not in age_mapping:
#             return "unknown"
#         else:
#             return age_mapping[age]


# def clean_epc_data(df):
#     """Standardise and clean EPC data.
#     For example, reformat dates and standardise categories.

#     Parameters
#     ----------
#     df : pandas.DataFrame
#         Raw/original EPC dataframe.

#     Return
#     ----------
#     df : pandas.DataFrame
#         Standarised and cleaned EPC dataframe."""

#     make_numeric = [
#         "WIND_TURBINE_COUNT",
#         "MAIN_HEATING_CONTROLS",
#         "MULTI_GLAZE_PROPORTION",
#         "FLOOR_HEIGHT",
#         "EXTENSION_COUNT",
#         "NUMBER_HEATED_ROOMS",
#         "NUMBER_HABITABLE_ROOMS",
#     ]

#     # Replace values such as INVALID! or NODATA!
#     for column in df.columns:
#         if column in numeric_features:
#             df[column] = df[column].replace(invalid_values, np.nan)

#             if column not in ["WIND_TURBINE_COUNT", "FLOOR_LEVEL"] + make_numeric:
#                 df[column] = df[column].mask(df[column] < 0.0)
#         else:
#             df[column] = df[column].replace(invalid_values, "unknown")

#     for column in df.columns:
#         if column in make_numeric:
#             df[column] = pd.to_numeric(df[column])
#             df[column] = df[column].mask(df[column] < 0.0)

#     df = standardise_dates(df, date_features)

#     if "CONSTRUCTION_AGE_BAND" in df.columns:
#         df["CONSTRUCTION_AGE_BAND_ORIGINAL"] = df["CONSTRUCTION_AGE_BAND"].apply(
#             standardise_constr_age_original
#         )
