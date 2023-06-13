# File: asf_core_data/pipeline/preprocessing/feature_engineering.py
"""
Adding new features to EPC dataset.

FEATURE ENGINEERING GUIDELINES
------------------------------

Feel free to add any functions for engineering new features.
Make sure to group similar new featues within one function and
to get_additional_features() in order to take effect in the next run.

"""

# ----------------------------------------------------------------------------------

# Import
import pandas as pd
import numpy as np

from hashlib import md5

from asf_core_data.getters.supplementary_data.geospatial import coordinates
from asf_core_data.pipeline.preprocessing import data_cleaning
from datetime import datetime

# ----------------------------------------------------------------------------------

other_heating_system = [
    "boiler and radiator",
    "boiler and underfloor",
    "community scheme",
    "heater",  # not specified heater
]

other_hp_expressions = [
    "heat pump",
    "pumpa teas",
    "pwmp gwres",  # starts with p
    "bwmp gwres",  # different from above, starts with b
    "pympiau gwres",  # starts with p
    "bympiau gwres",  # different from above, starts with b
]

# ----------------------------------------------------------------------------------


def short_hash(text):
    """Generate a unique short hash for given string.
    Legacy code. No longer used in newer versions.

    Args:
        text (str):  String for which to get ID.

    Returns:
        int: Unique ID.
    """

    hx_code = md5(text.encode()).hexdigest()
    int_code = int(hx_code, 16)
    short_code = str(int_code)[:16]
    return int(short_code)


def get_unique_building_id(df, short_code=False):
    """Add unique building ID column to dataframe.
    Legacy code. No longer used in newer versions.

    Args:
        df (str): String for which to get ID.
        short_code (bool, optional): Whether to add additional feature with short unique code. Defaults to False.

    Returns:
        str: Unique ID.
    """

    if ("ADDRESS1" not in df.columns) or ("POSTCODE" not in df.columns):
        return df

    # Remove samples with no address
    df.dropna(subset=["ADDRESS1"], inplace=True)

    # Create unique address and building ID
    df["BUILDING_ADDRESS_ID"] = df["ADDRESS1"].str.upper() + df["POSTCODE"].str.upper()

    if short_code:
        df["BUILDING_ADDRESS_ID"] = df["BUILDING_ADDRESS_ID"].apply(short_hash)

    return df


def get_new_epc_rating_features(df):
    """Get new EPC rating features related to EPC ratings

        CURR_ENERGY_RATING_NUM: EPC rating representeed as number
        high number = high rating.

        ENERGY_RATING_CAT: EPC range category.
        A-B, C-D or E-G

        DIFF_POT_ENERGY_RATING: Difference potential and current
        energy rating.

    Args:
            df (pandas.DataFrame): EPC dataframe.

        Returns:
            pandas.DataFrame: Updated EPC dataframe with new EPC rating features.
    """

    # EPC rating dict
    rating_dict = {
        "A": 7,
        "B": 6,
        "C": 5,
        "D": 4,
        "E": 3,
        "F": 2,
        "G": 1,
        "unknown": float("NaN"),
    }

    # EPC range cat dict
    EPC_cat_dict = {
        "A": "A-B",
        "B": "A-B",
        "C": "C-D",
        "D": "C-D",
        "E": "E-G",
        "F": "E-G",
        "G": "E-G",
        "unknown": "unknown",
    }

    if "CURRENT_ENERGY_RATING" not in df.columns:
        return df

    # EPC rating in number instead of letter
    df["CURR_ENERGY_RATING_NUM"] = df.CURRENT_ENERGY_RATING.map(rating_dict)

    # EPC rating in category (A-B, C-D or E-G)
    df["ENERGY_RATING_CAT"] = df.CURRENT_ENERGY_RATING.map(EPC_cat_dict)

    if "POTENTIAL_ENERGY_RATING" in df.columns:
        # Numerical difference between current and potential energy rating (A-G)
        df["DIFF_POT_ENERGY_RATING"] = (
            df.POTENTIAL_ENERGY_RATING.map(rating_dict) - df["CURR_ENERGY_RATING_NUM"]
        )
        # Filter out unreasonable values (below 0 and above 7)
        df.loc[
            ((df.DIFF_POT_ENERGY_RATING < 0.0) | (df.DIFF_POT_ENERGY_RATING > 7)),
            "DIFF_POT_ENERGY_RATING",
        ] = np.nan

    return df


def map_quality_to_score(df, list_of_features):
    """Map quality string tag (e.g. 'very good') to score and add it as a new feature.

    Args:
        df ( pandas.DataFrame): Dataframe to update with quality score.
        list_of_features (list): A list of dataframe features to update.


    Returns:
        pandas.DataFrame:  Updated dataframe with new score features.
    """

    quality_to_score_dict = {
        "Very Good": 5.0,
        "Good": 4.0,
        "Average": 3.0,
        "Poor": 2.0,
        "Very Poor": 1.0,
    }

    for feature in list_of_features:
        df[feature + "_AS_SCORE"] = df[feature].map(quality_to_score_dict)

    return df


def map_rating_to_cat(rating):
    """Map EPC rating in numbers (between 1.0 and 7.0) to EPC category range.

    Args:
        rating (float): EPC rating - usually average scores.

    Returns:
        str: EPC category range, e.g. A-B.
    """

    if rating < 2.0:
        return "F-G"
    elif rating >= 2.0 and rating < 3.0:
        return "E-F"
    elif rating >= 3.0 and rating < 4.0:
        return "D-E"
    elif rating >= 4.0 and rating < 5.0:
        return "C-D"
    elif rating >= 5.0 and rating < 6.0:
        return "B-C"
    elif rating >= 6.0 and rating < 7.0:
        return "A-B"


def get_heating_system(mainheat_description: str) -> str:
    """
    Extracts heating system from MAINHEAT_DESCRIPTION.

    Args:
        mainheat_description: EPC's MAINHEAT_DESCRIPTION value
    Returns:
        Property's heating system information.
    """
    if pd.isnull(mainheat_description):
        return "unknown"
    else:
        heating = mainheat_description.lower()
        heating = heating.replace(" & ", " and ")

        if ("ground source heat pump" in heating) or (
            "ground sourceheat pump" in heating
        ):
            return "ground source heat pump"

        elif ("air source heat pump" in heating) or ("air sourceheat pump" in heating):
            return "air source heat pump"

        elif "water source heat pump" in heating:
            return "water source heat pump"

        elif "community heat pump" in heating:
            return "community heat pump"

        elif any(hp_expression in heating for hp_expression in other_hp_expressions):
            return "heat pump"

        elif "warm air" in heating:
            return "warm air"

        elif "electric storage heaters" in heating:
            return "storage heater"

        elif "electric underfloor heating" in heating:
            return "underfloor heating"

        elif ("boiler and radiator" in heating) and (
            "boiler and underfloor" in heating
        ):
            return "boiler, radiator and underfloor"

        elif any(exp in heating for exp in other_heating_system):
            # If heating system word is found, save respective system type
            for word in other_heating_system:
                if word in heating:
                    return word

        return "unknown"


def get_heating_fuel(mainheat_description: str) -> str:
    """
    Extracts heating fuel from MAINHEAT_DESCRIPTION.

    Args:
        mainheat_description: EPC's MAINHEAT_DESCRIPTION value
    Returns:
        Property's heating fuel information.
    """
    if pd.isnull(mainheat_description):
        return "unknown"
    else:
        heating = mainheat_description.lower()
        heating = heating.replace(" & ", " and ")

        if ("ground source heat pump" in heating) or (
            "ground sourceheat pump" in heating
        ):
            return "electric"

        elif ("air source heat pump" in heating) or ("air sourceheat pump" in heating):
            return "electric"

        elif "water source heat pump" in heating:
            return "electric"

        elif "community heat pump" in heating:
            return "electric"

        elif any(hp_expression in heating for hp_expression in other_hp_expressions):
            return "electric"

        elif "warm air" in heating:
            return "electric"

        elif "electric storage heaters" in heating:
            return "electric"

        elif "electric underfloor heating" in heating:
            return "electric"

        elif any(exp in heating for exp in other_heating_system):
            # Set heating source dict
            heating_source_dict = {
                "gas": "gas",
                ", oil": "oil",  # with preceeding comma (!= "boiler")
                "lpg": "LPG",
                "electric": "electric",
            }

            # If heating source word is found, save respective source type
            for word, source in heating_source_dict.items():
                if word in heating:
                    return source

    return "unknown"


def get_heating_features(df, fine_grained_HP_types=False):
    """Updates EPC df with heating features:
    HEATING_SYSTEM: heat pump, boiler, community scheme, etc.
    HP_INSTALLED: True if heat pump as heating system, False if not;
    HEATING_FUEL: oil, gas, LPC, electric, etc.
    HP_TYPE: heat pump type when applicable ("air source heat pump", etc. and "No HP" if no heat pump as heating system),

    Args:
        df (pandas.DataFrame): EPC dataframe that is updated with heating features.
        fine_grained_HP_types (bool, optional):
            If True, HEATING_SYSTEM will contain detailed heat pump types (air sourced, ground sourced etc.).
            If False, return "heat pump" as heating type category. Defaults to False.
    Returns:
        pandas.DataFrame: Updated dataframe with heating system and source.
    """

    if "MAINHEAT_DESCRIPTION" not in df.columns:
        return df

    df["HEATING_SYSTEM"] = df["MAINHEAT_DESCRIPTION"].apply(get_heating_system)
    df["HP_INSTALLED"] = np.where(
        df["HEATING_SYSTEM"].str.contains("heat pump"), True, False
    )
    df["HEATING_FUEL"] = df["MAINHEAT_DESCRIPTION"].apply(get_heating_fuel)
    df["HP_TYPE"] = np.where(df["HP_INSTALLED"], df["HEATING_SYSTEM"], "No HP")

    if not fine_grained_HP_types:
        df["HEATING_SYSTEM"] = np.where(
            df["HEATING_SYSTEM"].str.contains("heat pump", case=False),
            "heat pump",
            df["HEATING_SYSTEM"],
        )

    # Also consider SECONDHEAT_DESCRIPTION and other languages
    df["HP_INSTALLED"] = np.where(
        (df["HP_INSTALLED"])
        | (df["SECONDHEAT_DESCRIPTION"].str.contains("heat pump", case=False))
        | (df["SECONDHEAT_DESCRIPTION"].str.contains("pumpa teas", case=False))
        | (df["SECONDHEAT_DESCRIPTION"].str.contains("pwmp gwres", case=False))
        | (df["SECONDHEAT_DESCRIPTION"].str.contains("bwmp gwres", case=False))
        | (df["SECONDHEAT_DESCRIPTION"].str.contains("pympiau gwres", case=False))
        | (df["SECONDHEAT_DESCRIPTION"].str.contains("bympiau gwres", case=False)),
        True,
        False,
    )

    # Update HP_TYPE, HEATING_SYSTEM and HEATING_FUEL after the changes to HP_INSTALLED
    df["HEATING_SYSTEM"] = np.where(
        df["HP_INSTALLED"] & (df["HP_TYPE"] == "No HP"),
        "heat pump",
        df["HEATING_SYSTEM"],
    )
    df["HEATING_FUEL"] = np.where(
        df["HP_INSTALLED"] & (df["HP_TYPE"] == "No HP"), "electric", df["HEATING_FUEL"]
    )
    df["HP_TYPE"] = np.where(
        df["HP_INSTALLED"] & (df["HP_TYPE"] == "No HP"), "heat pump", df["HP_TYPE"]
    )

    return df


def get_postcode_coordinates(df, postcode_field_name="POSTCODE"):
    """Add coordinates (longitude and latitude) to the dataframe
    based on the postcode.

    Args:
        df (pandas.DataFrame): EPC dataframe.
        postcode_field_name: Field name for postcode. Defaults to 'POSTCODE'.

    Returns:
        pandas.DataFrame: Same dataframe with longitude and latitude columns added.
    """

    # Get postcode/coordinates
    postcode_coordinates_df = coordinates.get_postcode_coordinates(
        desired_postcode_name=postcode_field_name
    )

    # Reformat POSTCODE
    postcode_coordinates_df = data_cleaning.reformat_postcode(
        postcode_coordinates_df,
        postcode_var_name=postcode_field_name,
        white_space="remove",
    )
    df = data_cleaning.reformat_postcode(df, white_space="remove")

    # Merge with location data
    df = pd.merge(df, postcode_coordinates_df, on=postcode_field_name, how="left")

    return df


def get_building_entry_feature(df, feature):
    """Create feature that shows number of entries for any given building
    based on BUILDING_REFERENCE_NUMBER or BUILDING_ID.

    Args:
        df (pandas.DataFrame): EPC dataframe.
        feature (str): Feature by which to count building entries.
            Needs to be "BUILDING_REFERNCE_NUMBER" or "BUILDING_ID" or "UPRN".

    Returns:
        pandas.DataFrame: EPC dataframe with # entry feature.
    """

    # Catch invalid inputs
    if feature not in ["BUILDING_REFERENCE_NUMBER", "BUILDING_ID", "UPRN"]:
        raise IOError("Feature '{}' is not a valid feature.".format(feature))

    feature_name_dict = {
        "BUILDING_REFERENCE_NUMBER": "N_ENTRIES",
        "BUILDING_ID": "N_ENTRIES_BUILD_ID",
        "UPRN": "N_SAME_UPRN_ENTRIES",
    }

    # Get name of new feature
    new_feature_name = feature_name_dict[feature]

    df[new_feature_name] = df[feature].map(dict(df.groupby(feature).size()))

    return df


def get_additional_features(df):
    """Add new features to the EPC dataset.
    The new features include information about the inspection and entry date,
    building references, fine-grained heating system features and differences in EPC ratings.

    Args:
        df (pandas.DataFrame): EPC dataframe.

    Returns:
        pandas.DataFrame: Updated dataframe with new features.
    """

    df = get_unique_building_id(df)
    df = get_building_entry_feature(df, "UPRN")

    df = get_heating_features(df)
    df = get_new_epc_rating_features(df)

    return df
