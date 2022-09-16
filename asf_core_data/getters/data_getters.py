import boto3
import os
from fnmatch import fnmatch
from urllib.request import urlretrieve
from zipfile import ZipFile
from asf_core_data import PROJECT_DIR
from asf_core_data import Path
import geopandas as gpd
import pandas as pd
import logging
import pickle
import json
import shutil


from asf_core_data.config import base_config
from asf_core_data.getters.epc import data_batches

# %%
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


def print_download_options():

    for key in data_dict.keys():
        print(key)


def get_dir_content(dir_name, base_name_only=False):

    s3_client = boto3.client("s3")
    folders = set()
    response = s3_client.list_objects_v2(Bucket="asf-core-data", Prefix=dir_name)

    for content in response.get("Contents", []):
        folders.add(os.path.dirname(content["Key"]))

    if base_name_only:
        folders = [Path(f).name for f in folders]

    return sorted(folders)


def load_data(
    data_path="S3",
    bucket_name="asf-core-data",
    file_path="",
    usecols=None,
    dtype=None,
    low_memory=False,
    skiprows=None,
    encoding=None,
    n_samples=None,
):

    if str(data_path) == "S3":
        loaded_data = load_s3_data(
            bucket_name,
            file_path,
            usecols=usecols,
            dtype=dtype,
            low_memory=low_memory,
            skiprows=skiprows,
            encoding=encoding,
            n_samples=n_samples,
        )
    else:
        file_path = data_path / file_path
        loaded_data = pd.read_csv(
            file_path,
            usecols=usecols,
            dtype=dtype,
            low_memory=low_memory,
            skiprows=skiprows,
            encoding=encoding,
            nrows=n_samples,
        )

    return loaded_data


def get_s3_dir_files(
    s3=boto3.resource("s3"),
    bucket_name="asf-core-data",
    dir_name=".",
    direct_child_only=False,
):
    """
    get a list of all files in bucket directory.
    s3: S3 boto3 resource
    bucket_name: The S3 bucket name
    dir_name: bucket directory name
    """
    dir_files = []
    my_bucket = s3.Bucket(bucket_name)
    for object_summary in my_bucket.objects.filter(Prefix=dir_name):
        dir_files.append(object_summary.key)

    if direct_child_only:
        s3 = boto3.client("s3")
        result = s3.list_objects(Bucket=bucket_name, Prefix=dir_name, Delimiter="/")
        dir_files = [o for o in result]

    return dir_files


def load_s3_data(
    bucket_name,
    file_name,
    usecols=None,
    dtype=None,
    low_memory=False,
    skiprows=None,
    encoding=None,
    n_samples=None,
):
    """
    Load data from S3 location.
    bucket_name: The S3 bucket name
    file_name: S3 key to load
    usecols: Columns of data to use. Defaults to None, loading all columns.
    """
    if fnmatch(file_name, "*.xlsx"):
        data = pd.read_excel(
            os.path.join("s3://" + bucket_name, file_name), sheet_name=None
        )
        if len(data) > 1:
            return data
        else:
            return data[list(data.keys())[0]]
    elif fnmatch(file_name, "*.csv"):
        return pd.read_csv(
            os.path.join("s3://" + bucket_name, file_name),
            encoding="latin-1",
            usecols=usecols,
            dtype=dtype,
            low_memory=low_memory,
            skiprows=skiprows,
            nrows=n_samples,
        )

    elif fnmatch(file_name, "*.geojson"):
        return gpd.read_file(os.path.join("s3://" + bucket_name, file_name))

    elif fnmatch(file_name, "*.pickle") or fnmatch(file_name, "*.pkl"):
        obj = s3.Object(bucket_name, file_name)
        file = obj.get()["Body"].read()
        return pickle.loads(file)

    else:
        print(
            'Function not supported for file type other than "*.xlsx", "*.pickle", and "*.csv"'
        )


def save_to_s3(s3, bucket_name, output_var, output_file_path):
    """
    Save data to S3 location.
    s3: S3 boto3 resource
    bucket_name: The S3 bucket name
    output_var: Object to save
    output_file_dir: S3 Directory to save object.
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


def download_s3_folder(s3_folder, local_dir):
    """
    Download the contents of a folder directory
    Args:
        bucket_name: the name of the s3 bucket
        s3_folder: the folder path in the s3 bucket
        local_dir: a relative or absolute directory path in the local file system
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("asf-core-data")
    for obj in bucket.objects.filter(Prefix=s3_folder):

        target = os.path.join(local_dir, s3_folder, os.path.relpath(obj.key, s3_folder))
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        if obj.key[-1] == "/":
            continue
        bucket.download_file(obj.key, target)


def download_core_data(version, local_dir, batch=None, unzip=True):

    if version.endswith(".csv"):
        data_to_load = version + ".zip"
    elif version.endswith(".zip"):
        data_to_load = version
    elif version == "supplementary_data":
        download_s3_folder(data_dict[version], local_dir)
        return
    else:
        data_to_load = str(data_dict[version]) + ".zip"

    s3_path = data_batches.get_batch_path(data_to_load, "S3", batch=batch)

    output_path = Path(local_dir) / s3_path

    Path(output_path.parent).mkdir(parents=True, exist_ok=True)
    download_from_s3(str(s3_path), str(output_path))

    if unzip:
        with ZipFile(output_path, "r") as zip_ref:
            zip_ref.extractall(output_path.parent)

        os.remove(output_path)

        trash_dir = output_path.parent / "__MACOSX/"

        if trash_dir.exists() and trash_dir.is_dir():
            shutil.rmtree(trash_dir)


def download_from_s3(path_to_file, output_path):

    s3 = boto3.client("s3")
    s3.download_file(Bucket="asf-core-data", Key=path_to_file, Filename=output_path)
