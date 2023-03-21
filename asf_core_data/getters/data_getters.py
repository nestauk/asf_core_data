import boto3
import os
from fnmatch import fnmatch
from zipfile import ZipFile
import pandas as pd
import logging
import pickle
import json
import shutil

import geopandas as gpd
import pandas as pd

from asf_core_data import Path
from asf_core_data.config import base_config
from asf_core_data.getters.epc import data_batches
from asf_core_data.getters import data_download

s3 = boto3.resource("s3")
logger = logging.getLogger(__name__)

data_dict = {
    "epc_raw": base_config.RAW_DATA_FILE,
    "epc_raw_combined": base_config.RAW_EPC_DATA_PATH,
    "epc_preprocessed_dedupl": base_config.PREPROC_EPC_DATA_DEDUPL_PATH,
    "epc_preprocessed": base_config.PREPROC_EPC_DATA_PATH,
    "EST_cleansed": base_config.EST_CLEANSED_EPC_DATA_PATH,
    "EST_cleansed_dedupl": base_config.EST_CLEANSED_EPC_DATA_DEDUPL_PATH,
    "supplementary_data": base_config.SUPPL_DATA_PATH,
}


def download_core_data(dataset, local_dir, batch=None, unzip=True):
    """Download the ASF core data from the S3 bucket to local directory.
    You can pass a path to a specific file or pick from the following predefined sets:

    - epc_raw
    - epc_raw_combined
    - epc_preprocessed_dedupl
    - epc_preprocessed
    - EST_cleansed
    - EST_cleansed_dedupl
    - supplementary_data

    Args:
        dataset (str): Which dataset to download. File path ending in .csv or .zip
            or one of the following options as str: epc_raw, epc_raw_combined,
            epc_preprocessed_dedupl, epc_preprocessed,
            EST_cleansed, EST_cleansed_dedupl, supplementary_data.
        local_dir (str/Path): Path to local directory.
        batch (str, optional): Batch name. Defaults to None (=newest).
        unzip (bool, optional): Whether or not to unzip downloaded folder (if zip file). Defaults to True.
    """

    if dataset.endswith(".csv"):
        data_to_load = dataset + ".zip"
    elif dataset.endswith(".zip"):
        data_to_load = dataset

    # Download entire folder
    elif dataset == "supplementary_data":
        data_download.download_s3_folder(data_dict[dataset], local_dir)
        return
    else:
        data_to_load = str(data_dict[dataset]) + ".zip"

    s3_path = data_batches.get_batch_path(data_to_load, "S3", batch=batch)

    output_path = Path(local_dir) / s3_path

    Path(output_path.parent).mkdir(parents=True, exist_ok=True)
    download_from_s3(str(s3_path), str(output_path))

    if unzip:
        with ZipFile(output_path, "r") as zip_ref:
            zip_ref.extractall(output_path.parent)

        os.remove(output_path)

    dirpath = Path(output_path.parent / "__MACOSX")

    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)


def print_download_options():
    """Print datasets that can be downloaded from asf-core-data S3 bucket."""

    for key in data_dict.keys():
        print(key)


def get_dir_content(path_to_dir, base_name_only=False):
    """Get contents of directory on S3 bucket asf-core-data.

    Args:
        path_to_dir (str/Path): Path to directory on S3 bucket.
        base_name_only (bool, optional): Get only name of files/subfolders, instead of full path. Defaults to False.

    Returns:
        contents (list): List of files or subfolders.
    """

    path_to_dir = str(path_to_dir)
    s3_client = boto3.client("s3")
    contents = set()
    response = s3_client.list_objects_v2(
        Bucket="asf-core-data", Prefix=str(path_to_dir)
    )

    for content in response.get("Contents", []):
        contents.add(Path(content["Key"]))

    if base_name_only:
        contents = [Path(f).name for f in contents]

    return sorted(contents)


def load_data(
    file_path,
    data_path="S3",
    bucket_name="asf-core-data",
    usecols=None,
    n_samples=None,
    dtype=None,
    skiprows=None,
    encoding=None,
    low_memory=False,
):
    """Load files from S3 bucket or local directory.

    Args:
        file_path (str, optional): Relative path to file to load.
        data_path (str/Path, optional): Path to ASF core data directory or 'S3'. Defaults to "S3".
        bucket_name (str, optional): S3 bucket name. Defaults to "asf-core-data".
        usecols (list, optional): Features/columns to load from EPC dataset. Defaults to None, loading all features.
        n_samples (int, optional): Number of samples/rows to load. Defaults to None, loading all samples.
        dtype (dict, optional): Dict with dtypes for easier loading. Defaults to None.
        skiprows (int, optional): Which rows in csv file to skip loading. Defaults to None.
        encoding (str, optional): Encoding for loading data. Defaults to None.
        low_memory (bool, optional): Whether to load data with low memory. Defaults to True.
            If True, internally process the file in chunks, resulting in lower memory use while parsing,
            but possibly mixed type inference.
            To ensure no mixed types either set False, or specify the type with the dtype parameter.

    Returns:
        loaded_data (pd.DataFrame): Data loaded as pandas dataframe.
    """

    if str(data_path) == "S3":
        loaded_data = load_s3_data(
            bucket_name,
            file_path,
            usecols=usecols,
            dtype=dtype,
            low_memory=low_memory,
            skiprows=skiprows,
            n_samples=n_samples,
        )
    else:
        full_path = Path(data_path) / file_path

        loaded_data = pd.read_csv(
            full_path,
            usecols=usecols,
            dtype=dtype,
            low_memory=low_memory,
            skiprows=skiprows,
            encoding=encoding,
            nrows=n_samples,
        )

    return loaded_data


def get_s3_dir_files(
    bucket_name="asf-core-data",
    path_to_dir=".",
):
    """Get a list of all files in given bucket directory.

    Args:
        bucket_name (str, optional): Bucket name on S3. Defaults to "asf-core-data".
        path_to_dir (str, optional): Path to directory of interest. Defaults to ".".

    Returns:
        dir_files (list): Files in given directory.
    """

    dir_files = []

    s3 = boto3.resource("s3")

    my_bucket = s3.Bucket(bucket_name)
    for object_summary in my_bucket.objects.filter(Prefix=path_to_dir):
        dir_files.append(object_summary.key)

    return dir_files


def load_s3_data(
    bucket_name,
    file_name,
    usecols=None,
    dtype=None,
    skiprows=None,
    n_samples=None,
    columns_to_parse_as_dates=None,
    encoding="latin-1",
    low_memory=False,
):
    """Load data from S3 location.

    Args:
        bucket_name (str): Name of S3 bucket.
        file_name (str/Path): File to load.
        usecols (list, optional): Features/columns to load from EPC dataset. Defaults to None, loading all features.
        dtype (dict, optional): Dict with dtypes for easier loading. Defaults to None.
        skiprows (int, optional): Which rows in csv file to skip loading. Defaults to None.
        n_samples (int, optional): Number of samples/rows to load. Defaults to None, loading all samples.
        columns_to_parse_as_dates: Columns that should be parsed as dates (when reading as csv). Defauls to None.
        encoding: Encoding when reading as csv. Defaults to latin-1.
        low_memory (bool, optional): Whether to load data with low memory. Defaults to True.
            If True, internally process the file in chunks, resulting in lower memory use while parsing,
            but possibly mixed type inference.
            To ensure no mixed types either set False, or specify the type with the dtype parameter.
    """

    if fnmatch(file_name, "*.xlsx"):
        excel_file = pd.ExcelFile(os.path.join("s3://" + bucket_name, file_name))
        sheet_names = excel_file.sheet_names

        # if excel file has data in multiple sheets
        if len(sheet_names) > 1:
            data = pd.DataFrame()
            for sheet in sheet_names:
                aux = pd.read_excel(
                    os.path.join("s3://" + bucket_name, file_name),
                    sheet_name=sheet,
                    dtype=dtype,
                )
                data = pd.concat([data, aux])
                del aux
        else:  # only one sheet
            data = pd.read_excel(
                os.path.join("s3://" + bucket_name, file_name),
                sheet_name=None,
                dtype=dtype,
            )
        if len(data) > 1:
            return data
        else:
            return data[list(data.keys())[0]]
    elif fnmatch(file_name, "*.csv"):
        return pd.read_csv(
            os.path.join("s3://" + bucket_name, file_name),
            encoding=encoding,
            usecols=usecols,
            dtype=dtype,
            low_memory=low_memory,
            skiprows=skiprows,
            nrows=n_samples,
            parse_dates=columns_to_parse_as_dates,
        )

    elif fnmatch(file_name, "*.geojson"):
        return gpd.read_file(os.path.join("s3://" + bucket_name, file_name))

    elif fnmatch(file_name, "*.pickle") or fnmatch(file_name, "*.pkl"):
        obj = s3.Object(bucket_name, file_name)
        file = obj.get()["Body"].read()
        return pickle.loads(file)

    else:
        print(
            'Function not supported for file type other than "*.xlsx", "*.pickle", "*.geojson", and "*.csv"'
        )


def save_to_s3(bucket_name, output_var, output_file_path):
    """Save object to S3 bucket.

    Args:
        bucket_name (str): Name of S3 bucket.
        output_var (str/Path): Object to save to S3.
        output_file_path (str/Path): Path for where to save file to.
    """

    obj = s3.Object(bucket_name, output_file_path)

    if fnmatch(output_file_path, "*.pkl") or fnmatch(output_file_path, "*.pickle"):
        byte_obj = pickle.dumps(output_var)
        obj.put(Body=byte_obj)
    elif fnmatch(output_file_path, "*.csv"):
        output_var.to_csv("s3://" + bucket_name + output_file_path, index=False)
    elif fnmatch(output_file_path, "*.json"):
        byte_obj = json.dumps(output_var)
        obj.put(Body=byte_obj)
    else:
        print(
            'Function not supported for file type other than "*.pkl", "*.json" and "*.csv"'
        )


def download_from_s3(path_to_file, output_path):
    """Download dataset from S3 bucket to local directory.

    Args:
        path_to_file (str/Path): Path to file or object to download.
        output_path (str/Path): Where to save it to.
    """

    s3 = boto3.client("s3")
    s3.download_file(Bucket="asf-core-data", Key=path_to_file, Filename=output_path)


def get_most_recent_batch_name(
    bucket: str,
    s3_folder_path: str,
    filter_keep_keywords: list = None,
    filter_remove_keywords: list = None,
) -> str:
    """
    Get the file name of the most recent batch for a specific set of files on an S3 folder.
    Args:
        bucket: s3 bucket e.g. "asf-core-data"
        s3_folder_path: path to S3 folder, e.g."/outputs/MCS/installers"
        filter_keep_keywords: keywords we should filter to keep
            e.g. ["installation", "installer"] -> will keep files containing either the word "installation" or "installer"
        filter_remove_keywords: keyword we should filter out
            e.g. ["historical"] -> will remove all files containing the keyword "historical"
    Returns:
        The most recent batch.
    """
    batches = [key for key in get_s3_dir_files(path_to_dir=s3_folder_path)]

    final_set = []
    if filter_keep_keywords is None and filter_remove_keywords is None:
        final_set = final_set + batches
    elif filter_remove_keywords is None:
        for f in filter_keep_keywords:
            final_set = final_set + [key for key in batches if f in key]
    elif filter_keep_keywords is None:
        for f in filter_remove_keywords:
            final_set = [key for key in batches if f not in key]
    else:
        for f in filter_keep_keywords:
            final_set = final_set + [key for key in batches if f in key]
        for f in filter_remove_keywords:
            final_set = [key for key in final_set if f not in key]

    if len(final_set) == 0:
        raise IOError("No files found.")

    # Return highest value since all files are marked with date stamps in format yyyymmdd
    return max(final_set)
