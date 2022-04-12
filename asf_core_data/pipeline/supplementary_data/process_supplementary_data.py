#########################################################
import pandas as pd
import geopandas as gpd
import os
from urllib.request import urlretrieve
import zipfile

from asf_core_data.getters.data_getters import s3, load_s3_data, save_to_s3
from asf_core_data import PROJECT_DIR, bucket_name, config

#########################################################

pop_colnames_dict = {
    "TIME": "year",
    "GEO": "geo",
    "Value": "population",
    "NUTS_ID": "nuts_id",
    "NUTS_NAME": "nuts_name",
}


def load_nuts_data(shape_url, shapefile_path, nuts_file, epsg):

    full_shapefile_path = str(PROJECT_DIR) + shapefile_path
    if not os.path.isdir(full_shapefile_path):
        os.mkdir(full_shapefile_path)

    zip_path, _ = urlretrieve(shape_url)
    with zipfile.ZipFile(zip_path, "r") as zip_files:
        for zip_names in zip_files.namelist():
            if zip_names == nuts_file:
                zip_files.extract(zip_names, path=full_shapefile_path)
                nuts_geo = gpd.read_file(full_shapefile_path + nuts_file)
                nuts_geo = nuts_geo[nuts_geo["CNTR_CODE"] == "UK"].reset_index(
                    drop=True
                )

    return nuts_geo


def process_population_density_data(raw_population_data):
    """preprocesses population data to:
    - extract new columns;
    - drop unnecessary columns and rows;
    - rename columns;
    - return UK populations.
    """
    # drop empty values
    raw_population_data = raw_population_data[
        raw_population_data["Value"] != ":"
    ].reset_index(drop=True)
    # get most recent population in df that is in the UK
    raw_population_data["Value"] = (
        raw_population_data["Value"].str.replace(",", "").astype(int)
    )
    raw_population_data["TIME"] = pd.to_datetime(
        raw_population_data["TIME"], format="%Y"
    )
    raw_population_data["GEO"] = (
        raw_population_data["GEO"]
        .str.lower()
        .str.replace("—", " ")
        .str.replace("-", " ")
        .str.replace("(uk)", "")
        .str.replace("(", "")
        .str.replace(")", "")
        .str.strip()
    )
    # merge population data with nuts geo information
    nuts_geo = load_nuts_data(shape_url, shapefile_path, nuts_file, epsg)
    nuts_geo["NUTS_NAME"] = (
        nuts_geo["NUTS_NAME"].str.lower().str.replace("—", " ").str.replace("-", " ")
    )

    merged_nuts_uk_popluation_data = pd.merge(
        raw_population_data, nuts_geo, how="right", left_on="GEO", right_on="NUTS_NAME"
    )
    merged_nuts_uk_popluation_data = merged_nuts_uk_popluation_data[
        ["TIME", "GEO", "Value", "NUTS_ID", "NUTS_NAME", "geometry"]
    ]
    # choose smaller west midlands population based on other sources
    merged_nuts_uk_popluation_data = merged_nuts_uk_popluation_data.drop(
        merged_nuts_uk_popluation_data[
            merged_nuts_uk_popluation_data["GEO"].str.contains("west midlands")
        ]["Value"].idxmax()
    )
    merged_nuts_uk_popluation_data = merged_nuts_uk_popluation_data[
        merged_nuts_uk_popluation_data["TIME"]
        == merged_nuts_uk_popluation_data["TIME"].max()
    ]

    merged_nuts_uk_popluation_data = merged_nuts_uk_popluation_data.rename(
        columns=pop_colnames_dict
    )
    return merged_nuts_uk_popluation_data


if __name__ == "__main__":
    # get config file with relevant paramenters
    shape_url = config["SHAPE_URL"]
    shapefile_path = config["SHAPEFILE_PATH"]
    nuts_file = config["NUTS_FILE"]
    epsg = config["EPSG"]

    raw_population_path = config["RAW_POPULATION_PATH"]
    clean_population_path = config["CLEAN_POPULATION_PATH"]

    raw_population_data = load_s3_data(s3, bucket_name, raw_population_path)
    clean_population_data = process_population_density_data(raw_population_data)
    save_to_s3(s3, bucket_name, clean_population_data, clean_population_path)
