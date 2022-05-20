# File: getters/location_data.py
"""Loading location data, e.g. postcodes with coordinates."""

# ---------------------------------------------------------------------------------

import pandas as pd
from asf_core_data import get_yaml_config, Path, PROJECT_DIR
from asf_core_data.config import base_config

# ---------------------------------------------------------------------------------


def get_postcode_coordinates(
    data_path=PROJECT_DIR, rel_data_path=base_config.POSTCODE_TO_COORD_PATH
):
    """Load location data (postcode, latitude, longitude).

    Args:
        data_path (str/Path, optional): Path to ASF core data directory or 'S3'. Defaults to PROJECT_DIR.
        rel_data_path (str/Path, optional): Relative path for geographical data. Defaults to base_config.POSTCODE_TO_COORD_PATH.

    Returns:
        pandas.DateFrame: Location data (postcode, latitude, longitude).
    """

    path = Path(data_path) / rel_data_path

    # Load data
    postcode_coordinates_df = pd.read_csv(path)

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
