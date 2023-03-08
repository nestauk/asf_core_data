# File: getters/location_data.py
"""Loading location data, e.g. postcodes with coordinates."""

# ---------------------------------------------------------------------------------

from asf_core_data.config import base_config
from asf_core_data.getters import data_getters

# ---------------------------------------------------------------------------------


def get_postcode_coordinates(
    data_path="S3",
    rel_data_path=base_config.POSTCODE_TO_COORD_PATH,
    desired_postcode_name="POSTCODE",
):
    """Load location data (postcode, latitude, longitude).
    Field names are POSTCODE, LATITUDE and LONGITUDE by default,
    but you can change the POSTCODE field name.

    Args:
        data_path (str/Path, optional): Path to ASF core data directory or 'S3'. Defaults to "S3".
        rel_data_path (str/Path, optional): Relative path for geographical data. Defaults to base_config.POSTCODE_TO_COORD_PATH.
        desired_postcode_name: Field name for postcode. Defaults to 'POSTCODE'.

    Returns:
        pandas.DateFrame: Location data (postcode, latitude, longitude).
    """

    postcode_coordinates_df = data_getters.load_data(rel_data_path, data_path=data_path)

    # Remove ID (not necessary and conflicts with EPC dataframe)
    del postcode_coordinates_df["id"]

    # Rename columns to match data
    postcode_coordinates_df = postcode_coordinates_df.rename(
        columns={
            "postcode": desired_postcode_name,
            "latitude": "LATITUDE",
            "longitude": "LONGITUDE",
        }
    )
    return postcode_coordinates_df
