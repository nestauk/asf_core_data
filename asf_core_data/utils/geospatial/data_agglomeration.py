import h3
import pandas as pd
from asf_core_data.getters.supplementary_data import coordinates
from asf_core_data.pipeline.preprocessing import data_cleaning


def add_hex_id(df, resolution=7.5):
    """Get H3 hex ID based on coordinates.

     Parameters
     ----------
    row : pandas.Series
        Dataframe row.

    resolution : int, default=7
        H3 resolution.

    Return
    ---------

        H3 index: int"""

    df["hex_id"] = df.apply(
        lambda row: h3.geo_to_h3(row["LATITUDE"], row["LONGITUDE"], resolution), axis=1
    )

    return df


def get_postcode_coordinates(df):
    """Add coordinates (longitude and latitude) to the dataframe
    based on the postcode.

    df : pandas.DataFrame
        EPC dataframe.

    Return
    ---------
    df : pandas.DataFrame
        Same dataframe with longitude and latitude columns added."""

    # Get postcode/coordinates
    postcode_coordinates_df = coordinates.get_postcode_coordinates()

    # Reformat POSTCODE
    df = data_cleaning.reformat_postcode(df)
    postcode_coordinates_df = data_cleaning.reformat_postcode(postcode_coordinates_df)

    postcode_coordinates_df["POSTCODE"] = (
        postcode_coordinates_df["POSTCODE"].str.upper().str.replace(" ", "")
    )

    # Merge with location data
    df = pd.merge(df, postcode_coordinates_df, on=["POSTCODE"])

    print(df.shape)

    return df


def get_cat_distr_grouped_by_agglo_f(df, feature, agglo_feature="hex_id"):

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

    hex_to_feature = get_cat_distr_grouped_by_agglo_f(
        df, feature, agglo_feature="hex_id"
    )[["hex_id", "MOST_FREQUENT_LOCAL_AUTHORITY_LABEL"]]

    hex_to_feature = hex_to_feature.rename(
        columns={"MOST_FREQUENT_" + feature: feature}
    )

    return hex_to_feature
