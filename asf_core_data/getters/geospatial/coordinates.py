# File: getters/location_data.py
"""Loading location data, e.g. postcodes with coordinates."""

# ---------------------------------------------------------------------------------

import pandas as pd
from asf_core_data import get_yaml_config, Path, PROJECT_DIR
from asf_core_data.config import base_config

# ---------------------------------------------------------------------------------

# # Load config file
# data_config = get_yaml_config(
#     Path(str(PROJECT_DIR) + "/asf_core_data/config/base.yaml")
# )

# # Get paths
# LOCATION_PATH = str(PROJECT_DIR) + data_config["POSTCODE_TO_COORD_PATH"]


def get_postcode_coordinates():
    """Load location data (postcode, latitude, longitude).

    Parameters
    ----------
    None

    Return
    ---------
    location_data_df : pandas.DateFrame
        Location data (postcode, latitude, longitude)."""

    # Load data
    postcode_coordinates_df = pd.read_csv(base_config.LOCATION_PATH)

    # Remove ID (not necessary and conflicts with EPC dataframe)
    del postcode_coordinates_df["id"]

    # Rename columns to match EPC data
    postcode_coordinates_df = postcode_coordinates_df.rename(
        columns={
            "postcode": "POSTCODE",
            "latitude": "LATITUDE",
            "longitude": "LONGITUDE",
        }
    )
    return postcode_coordinates_df
