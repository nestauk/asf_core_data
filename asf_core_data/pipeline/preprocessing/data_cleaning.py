# File: heat_pump_adoption_modelling/pipeline/preprocessing/data_cleaning.py
"""Cleaning and standardising the EPC dataset."""

from asf_core_data import PROJECT_DIR, get_yaml_config, Path
from asf_core_data.config import base_config
from numpy.core import numeric
import pandas as pd
import numpy as np


# Load config file
config = get_yaml_config(Path(str(PROJECT_DIR) + "/asf_core_data/config/base.yaml"))


def reformat_postcode(df):
    """Change the POSTCODE feature in uniform format (without spaces).

    Parameters
    ----------
    df : pandas.Dataframe
        Dataframe to format.

    Return
    ----------
    df : pandas.Dataframe
        Dataframe with reformatted POSTCODE."""

    df["POSTCODE"] = df["POSTCODE"].str.upper().str.replace(r"\s+", "", regex=True)

    return df


def standardise_dates(df, features):

    for feature in features:

        df[feature] = df[feature].str.replace(r"00(\d\d)", r"20\1", regex=True)

        df[feature] = pd.to_datetime(df[feature], errors="coerce")

        df.loc[
            (df[feature].dt.year > base_config.CURRENT_YEAR + 1)
            & (df[feature].dt.year < 2018),
            feature,
        ] = np.datetime64("NaT")

    return df


def date_formatter(date):
    """Reformat the date to the format year-month-day.

    Parameters
    ----------
    date: str/float
        Date to reformat (usually from INSPECTION_DATE).

    Return
    ----------
    formatted_date : str
        Formatted data in the required format."""

    if isinstance(date, float):
        return "unknown"

    # Get year, month and day from different formats
    if "-" in date:
        year, month, day = str(date).split("-")
    elif "/" in date:
        day, month, year = str(date).split("/")
    else:
        return "unknown"

    # Assume that years starting with 00xx mean 20xx
    if year.startswith("00"):
        year = "20" + year[-2:]

    # Discard entries past current year + plus (future projects) and before 2008
    if len(year) != 4 or int(year) > base_config.CURRENT_YEAR + 1 or int(year) < 2008:
        return "unknown"

    formatted_date = year + "/" + month + "/" + day

    return formatted_date


def standardise_solar_water_heating_flag(flag):

    flag_dict = {
        "N": "False",
        "unknown": "unknown",
        "Y": "True",
        "false": "False",
        "true": "True",
    }

    if flag not in flag_dict.keys():
        return "unknown"
    else:
        return flag_dict[flag]


def standardise_floor_level(floor_level, as_numeric=True):
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

    floor_level_dict = {
        "-1.0": -1,
        "Basement": -1,
        "Ground": 0,
        "0.0": 0,
        "00": 0,
        "0": 0,
        "1st": 1,
        "1.0": 1,
        "1": 1,
        "01": 1,
        "2nd": 2,
        "2.0": 2,
        "2": 2,
        "02": 2,
        "3rd": 3,
        "3.0": 2,
        "3": 3,
        "03": 3,
        "04": 4,
        "4th": 4,
        "4.0": 4,
        "4": 4,
        "05": 5,
        "5th": 5,
        "5.0": 5,
        "5": 5,
        "06": 6,
        "6th": 6,
        "6.0": 6,
        "6": 6,
        "07": 7,
        "7th": 7,
        "7.0": 7,
        "7": 7,
        "08": 8,
        "8th": 8,
        "8.0": 8,
        "8": 8,
        "09": 9,
        "9th": 9,
        "9.0": 9,
        "9": 9,
        "10": 10,
        "10th": 10,
        "10.0": 10,
        "11th": 10,
        "12th": 10,
        "13th": 10,
        "14th": 10,
        "15th": 10,
        "16th": 10,
        "17th": 10,
        "18th": 10,
        "19th": 10,
        "20th": 10,
        "11": 10,
        "12": 10,
        "13": 10,
        "14": 10,
        "15": 10,
        "16": 10,
        "17": 10,
        "18": 10,
        "19": 10,
        "20": 10,
        "21st or above": 10,
        "20+": 10,
        "top floor": 10,
        "mid floor": 5,
    }

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


def standardise_tenure(tenure):
    """Standardise tenure types; one of the four categories:
    rental (social), rental (private), owner-occupied, unknown

    Parameters
    ----------
    tenure : str
        Raw tenure type.

    Return
    ----------
    standardised tenure : str
        Standardised tenure type."""

    # Catch NaN
    if isinstance(tenure, float):
        return "unknown"

    tenure = tenure.lower()
    tenure_mapping = {
        "owner-occupied": "owner-occupied",
        "rental (social)": "rental (social)",
        "rented (social)": "rental (social)",
        "rental (private)": "rental (private)",
        "rented (private)": "rental (private)",
        "unknown": "unknown",
        "no data!": "unknown",
        "not defined - use in the case of a new dwelling for which the intended tenure in not known. it is no": "unknown",
    }

    return tenure_mapping[tenure]


def standardise_constr_age(age, adjust_age_bands=True):

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

    # Handle NaN
    if isinstance(age, float):
        return "unknown"

    age = age.strip()

    age_mapping = {
        "England and Wales: before 1900": "England and Wales: before 1900",
        "England and Wales: 1900-1929": "England and Wales: 1900-1929",
        "England and Wales: 1930-1949": "England and Wales: 1930-1949",
        "England and Wales: 1950-1966": "England and Wales: 1950-1966",
        "England and Wales: 1967-1975": "England and Wales: 1967-1975",
        "England and Wales: 1976-1982": "England and Wales: 1976-1982",
        "England and Wales: 1983-1990": "England and Wales: 1983-1990",
        "England and Wales: 1991-1995": "England and Wales: 1991-1995",
        "England and Wales: 1996-2002": "England and Wales: 1996-2002",
        "England and Wales: 2003-2006": "England and Wales: 2003-2006",
        "England and Wales: 2007-2011": "England and Wales: 2007-2011",
        "England and Wales: 2007-2011": "England and Wales: 2007 onwards",
        "England and Wales: 2007 onwards": "England and Wales: 2007 onwards",
        "England and Wales: 2012 onwards": "England and Wales: 2012 onwards",
        "1900": "England and Wales: 1900-1929",
        "2021": "England and Wales: 2012 onwards",
        "2020": "England and Wales: 2012 onwards",
        "2019": "England and Wales: 2012 onwards",
        "2018": "England and Wales: 2012 onwards",
        "2017": "England and Wales: 2012 onwards",
        "2016": "England and Wales: 2012 onwards",
        "2015": "England and Wales: 2012 onwards",
        "2014": "England and Wales: 2012 onwards",
        "2013": "England and Wales: 2012 onwards",
        "2012": "England and Wales: 2012 onwards",
        "2011": "England and Wales: 2007 onwards",
        "2010": "England and Wales: 2007 onwards",
        "2009": "England and Wales: 2007 onwards",
        "2008": "England and Wales: 2007 onwards",
        "2007": "England and Wales: 2007 onwards",
        "before 1919": "Scotland: before 1919",
        "1919-1929": "Scotland: 1919-1929",
        "1930-1949": "Scotland: 1930-1949",
        "1950-1964": "Scotland: 1950-1964",
        "1965-1975": "Scotland: 1965-1975",
        "1976-1983": "Scotland: 1976-1983",
        "1984-1991": "Scotland: 1984-1991",
        "1992-1998": "Scotland: 1992-1998",
        "1999-2002": "Scotland: 1999-2002",
        "2003-2007": "Scotland: 2003-2007",
        "2008 onwards": "Scotland: 2008 onwards",
        "unknown": "unknown",
        "NO DATA!": "unknown",
        "INVALID!": "unknown",
        "Not applicable": "unknown",
    }

    age_mapping_adjust_age_bands = {
        "England and Wales: before 1900": "England and Wales: before 1900",
        "England and Wales: 1900-1929": "1900-1929",
        "England and Wales: 1930-1949": "1930-1949",
        "England and Wales: 1950-1966": "1950-1966",
        "England and Wales: 1967-1975": "1965-1975",
        "England and Wales: 1976-1982": "1976-1983",
        "England and Wales: 1983-1990": "1983-1991",
        "England and Wales: 1991-1995": "1991-1998",
        "England and Wales: 1996-2002": "1996-2002",
        "England and Wales: 2003-2006": "2003-2007",
        "England and Wales: 2007-2011": "2007 onwards",
        "England and Wales: 2007 onwards": "2007 onwards",
        "England and Wales: 2012 onwards": "2007 onwards",
        "2021": "2007 onwards",
        "2020": "2007 onwards",
        "2019": "2007 onwards",
        "2018": "2007 onwards",
        "2017": "2007 onwards",
        "2016": "2007 onwards",
        "2015": "2007 onwards",
        "2015": "2007 onwards",
        "2014": "2007 onwards",
        "2013": "2007 onwards",
        "2013": "2007 onwards",
        "2012": "2007 onwards",
        "2011": "2007 onwards",
        "2010": "2007 onwards",
        "2009": "2007 onwards",
        "2008": "2007 onwards",
        "2007": "2007 onwards",
        "1900": "1900-1929",
        "before 1919": "Scotland: before 1919",
        "1919-1929": "1900-1929",
        "1930-1949": "1930-1949",
        "1950-1964": "1950-1966",
        "1965-1975": "1965-1975",
        "1976-1983": "1976-1983",
        "1984-1991": "1983-1991",
        "1992-1998": "1991-1998",
        "1999-2002": "1996-2002",
        "2003-2007": "2003-2007",
        "2008 onwards": "2007 onwards",
        "unknown": "unknown",
        "NO DATA!": "unknown",
        "INVALID!": "unknown",
        "Not applicable": "unknown",
    }

    if adjust_age_bands:

        if age not in age_mapping_adjust_age_bands:
            return "unknown"
        else:
            return age_mapping_adjust_age_bands[age]
    else:
        if age not in age_mapping:
            return "unknown"
        else:
            return age_mapping[age]


def standardise_constr_age_original(age):
    """Standardise construction age bands.

    Parameters
    ----------
    age : str
        Raw construction age.

    Return
    ----------
    Standardised age construction band : str
        Standardised age construction band."""

    return standardise_constr_age(age, adjust_age_bands=False)


def standardise_efficiency(efficiency):
    """Standardise efficiency types; one of the five categories:
    poor, very poor, average, good, very good

    Parameters
    ----------
    efficiency : str
        Raw efficiency type.

    Return
    ----------
    standardised efficiency : str
        Standardised efficiency type."""

    # Handle NaN
    if isinstance(efficiency, float):
        return "unknown"

    efficiency = efficiency.lower().strip()
    efficiency = efficiency.strip('"')
    efficiency = efficiency.strip()
    efficiency = efficiency.strip("|")
    efficiency = efficiency.strip()

    efficiency_mapping = {
        "poor |": "Poor",
        "very poor |": "Very Poor",
        "average |": "Average",
        "good |": "Good",
        "very good |": "Very Good",
        "poor": "Poor",
        "very poor": "Very Poor",
        "average": "Average",
        "good": "Good",
        "very good": "Very Good",
        "n/a": "unknown",
        "n/a |": "unknown",
        "n/a": "unknown",
        "n/a | n/a": "unknown",
        "n/a | n/a | n/a": "unknown",
        "n/a | n/a | n/a | n/a": "unknown",
        "no data!": "unknown",
        "unknown": "unknown",
    }

    return efficiency_mapping[efficiency]


def standardise_glazed_area(area, as_numeric=True):
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

    glazed_area_dict = {
        "1": 1,
        "3": 3,
        "2": 2,
        "5": 5,
        "4": 4,
        "0": np.nan,
        "Normal": 3,
        "unknown": np.nan,
        "More Than Typical": 4,
        "Much More Than Typical": 5,
        "Less Than Typical": 2,
        "Much Less Than Typical": 1,
        "Not Defined": np.nan,
        np.nan: np.nan,
    }

    glazed_area_cat_dict = {
        5: "Much More Than Typical",
        4: "More Than Typical",
        3: "Normal",
        2: "Less Than Typical",
        1: "Much Less Than Typical",
        np.nan: "unknown",
    }

    # Standardise (numeric or cat) using dict
    if area in glazed_area_dict.keys():
        if as_numeric:
            return glazed_area_dict[area]
        else:
            return glazed_area_cat_dict[glazed_area_dict[area]]

    # Handle unknown/NaN
    else:
        standardised_area = np.nan if as_numeric else "unknown"
        return standardised_area


def clean_photo_supply(df):
    """Extract photo supply area from length descriptions.

    Paramters
    ----------
    df : pandas.DataFrame
        Given dataframe.

    Return
    ---------
     df : pandas.DataFrame
        Dataframe with fixed photo supply column."""

    # Set all values mentioning "Peak Power" to "unknown" because they do not contain photo supply area
    df["PHOTO_SUPPLY"] = df["PHOTO_SUPPLY"].str.replace(
        r".*Peak Power.*", "unknown", regex=True
    )

    # Extract photo supply area from value
    df["PHOTO_SUPPLY"] = df["PHOTO_SUPPLY"].str.extract(r"(\d+)[\%|\.d$]").astype(float)
    return df


def clean_local_authority(local_authority):
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


def cap_feature_values(df, feature, cap_n=10):
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
    df.loc[(df[feature] >= cap_n), feature] = cap_n  # str(cap_n) + "+"

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
        "TENURE": standardise_tenure,
        "CONSTRUCTION_AGE_BAND": standardise_constr_age,
        "WINDOWS_ENERGY_EFF": standardise_efficiency,
        "FLOOR_ENERGY_EFF": standardise_efficiency,
        "HOT_WATER_ENERGY_EFF": standardise_efficiency,
        "LIGHTING_ENERGY_EFF": standardise_efficiency,
        "LOCAL_AUTHORITY_LABEL": clean_local_authority,
        "SOLAR_WATER_HEATING_FLAG": standardise_solar_water_heating_flag,
        "FLOOR_LEVEL": standardise_floor_level,
        "GLAZED_AREA": standardise_glazed_area,
    }

    date_features = ["LODGEMENT_DATE", "INSPECTION_DATE"]

    numeric_features = [
        "ENERGY_CONSUMPTION_CURRENT",
        "TOTAL_FLOOR_AREA",
        "CURRENT_ENERGY_EFFICIENCY",
        "CO2_EMISS_CURR_PER_FLOOR_AREA",
        "HEATING_COST_CURRENT",
        "HEATING_COST_POTENTIAL",
        "HOT_WATER_COST_CURRENT",
        "HOT_WATER_COST_POTENTIAL",
        "LIGHTING_COST_CURRENT",
        "LIGHTING_COST_POTENTIAL",
        "WIND_TURBINE_COUNT",
        "MAIN_HEATING_CONTROLS",
        "MULTI_GLAZE_PROPORTION",
        "FLOOR_HEIGHT",
        "EXTENSION_COUNT",
        "NUMBER_HEATED_ROOMS",
        "FLOOR_LEVEL",
        "NUMBER_HABITABLE_ROOMS",
        "CO2_EMISSIONS_CURRENT",
    ]

    invalid_values = [
        "unknown",
        "%%MAINHEATCONTROL%%",
        "NODATA!",
        "unknown",
        "NO DATA!",
        "INVALID!",
        "not defined",
        "none of the above",
        "no data!",
        "nodata!",
        "Not applicable",
        "Not Defined",
        "Unknown",
        "not recorded",
        np.nan,
    ]

    make_numeric = [
        "WIND_TURBINE_COUNT",
        "MAIN_HEATING_CONTROLS",
        "MULTI_GLAZE_PROPORTION",
        "FLOOR_HEIGHT",
        "EXTENSION_COUNT",
        "NUMBER_HEATED_ROOMS",
        "NUMBER_HABITABLE_ROOMS",
    ]

    # Replace values such as INVALID! or NODATA!
    for column in df.columns:
        if column in numeric_features:
            df[column] = df[column].replace(invalid_values, np.nan)

            if column not in ["WIND_TURBINE_COUNT", "FLOOR_LEVEL"] + make_numeric:
                df[column] = df[column].mask(df[column] < 0.0)
        else:
            df[column] = df[column].replace(invalid_values, "unknown")

    for column in df.columns:
        if column in make_numeric:
            df[column] = pd.to_numeric(df[column])
            df[column] = df[column].mask(df[column] < 0.0)

    df = standardise_dates(df, date_features)

    if "CONSTRUCTION_AGE_BAND" in df.columns:
        df["CONSTRUCTION_AGE_BAND_ORIGINAL"] = df["CONSTRUCTION_AGE_BAND"].apply(
            standardise_constr_age_original
        )

    # Reformat postcode
    if "POSTCODE" in df.columns:
        df = reformat_postcode(df)

    if "PHOTO_SUPPLY" in df.columns:
        df = clean_photo_supply(df)

    # Clean up features
    for column in df.columns:
        if column in column_to_function_dict.keys():
            df[column] = df[column].apply(column_to_function_dict[column])

    if "NUMBER_HABITABLE_ROOMS" in df.columns:
        # Limit max value for NUMBER_HABITABLE_ROOMS
        df = cap_feature_values(df, "NUMBER_HABITABLE_ROOMS", cap_n=10)

    return df
