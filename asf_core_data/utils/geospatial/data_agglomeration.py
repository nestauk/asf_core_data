# File: asf_core_data/utils/geospatial/data_agglomeration.py
"""Agglomerate data for given areas, e.g. local authorities or hex areas.
"""

# ---------------------------------------------------------------------------------
# Imports

import pandas as pd
import h3

from asf_core_data import PROJECT_DIR
from asf_core_data.getters.supplementary_data.geospatial import coordinates
from asf_core_data.pipeline.preprocessing import data_cleaning
from asf_core_data.config import base_config

# ---------------------------------------------------------------------------------


def add_hex_id(df, resolution=7.5):
    """Get H3 hex ID based on coordinates.

    Args:
        df (pandas.DataFrame): Dataframe.
        resolution (float, optional):  H3 resolution. Defaults to 7.5.

    Returns:
        df (pandas.DataFrame): Dataframe with new column "hex_id".
    """

    df["hex_id"] = df.apply(
        lambda row: h3.geo_to_h3(row["LATITUDE"], row["LONGITUDE"], resolution), axis=1
    )

    return df


def get_postcode_coordinates(
    df, data_path=PROJECT_DIR, rel_data_path=base_config.POSTCODE_TO_COORD_PATH
):
    """Add coordinates (longitude and latitude) to the dataframe
    based on the postcode.

    Args:
        df (pandas.DataFrame): Dataframet o which to add coordinates.
        data_path (str/Path, optional): Location to ASF core data. Defaults to PROJECT_DIR.
        rel_data_path (str/Path, optional): Relative location. Defaults to base_config.POSTCODE_TO_COORD_PATH.

    Returns:
        df (pandas.DataFrame): Same dataframe with longitude and latitude columns added
    """

    # Get postcode/coordinates
    postcode_coordinates_df = coordinates.get_postcode_coordinates(
        data_path=data_path, rel_data_path=rel_data_path
    )

    # Reformat POSTCODE
    df = data_cleaning.reformat_postcode(df)
    postcode_coordinates_df = data_cleaning.reformat_postcode(postcode_coordinates_df)

    postcode_coordinates_df["POSTCODE"] = (
        postcode_coordinates_df["POSTCODE"].str.upper().str.replace(" ", "")
    )

    # Merge with location data
    df = pd.merge(df, postcode_coordinates_df, on=["POSTCODE"])

    return df


def get_cat_distr_grouped_by_agglo_f(df, feature, agglo_feature="hex_id"):
    """For a given feature, group its categories/values by the agglomeration feature (e.g. area)
    and compute the percentage of each value and the most frequent one.

    Args:
        df (pandas.DataFrame): Dataframe of interest.
        feature (str): Feature of interest: its categories/values are processed.
        agglo_feature (str, optional): Feature by which to group/agglomerate. Defaults to "hex_id".

    Returns:
        pandas.DataFrame: Dataframe with category percentages agglomerated by agglo feature.
    """ """"""

    # Group the data by agglomeration feature (e.g. hex id)
    grouped_by_agglo_f = df.groupby(agglo_feature)

    # Count the samples per agglomeration feature category
    n_samples_agglo_cat = grouped_by_agglo_f[feature].count()

    # Get the feature categories by the agglomeration feature
    feature_cats_by_agglo_f = (
        df.groupby([agglo_feature, feature]).size().unstack(fill_value=0)
    )

    # Normalise by the total number of samples per category
    cat_percentages_by_agglo_f = (feature_cats_by_agglo_f.T / n_samples_agglo_cat).T

    # Get the most frequent feature cateogry
    cat_percentages_by_agglo_f[
        "MOST_FREQUENT_" + feature
    ] = cat_percentages_by_agglo_f.idxmax(axis=1)

    # Get the total
    cat_percentages_by_agglo_f[agglo_feature + "_TOTAL"] = n_samples_agglo_cat

    # Reset index for easier processing
    cat_percentages_by_agglo_f = cat_percentages_by_agglo_f.reset_index()

    return cat_percentages_by_agglo_f


def map_hex_to_feature(df, feature):
    """Get most frequent feature value for each hex_id,
    e.g. the most common local authority label for a hex area.

    Args:
        df (pd.DataFrame): Dataframe which includes both 'hex_id' and feature.
        feature (str): Feature to use for mapping.

    Returns:
        pd.DataFrame: 'hex_id' and most frequent feature value
    """

    hex_to_feature = get_cat_distr_grouped_by_agglo_f(
        df, feature, agglo_feature="hex_id"
    )[["hex_id", "MOST_FREQUENT_{}".format(feature)]]

    return hex_to_feature
