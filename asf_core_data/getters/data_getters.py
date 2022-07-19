import boto3
import os
from fnmatch import fnmatch
from urllib.request import urlretrieve
from zipfile import ZipFile
from asf_core_data import PROJECT_DIR
from asf_core_data import Path
import pandas as pd
import logging
import pickle
import json

# %%
s3 = boto3.resource("s3")
logger = logging.getLogger(__name__)
# get config file with relevant paramenters
# config_info = get_yaml_config(_base_config_path)


def get_dir_content(dir_name, base_name_only=False):

    s3_client = boto3.client("s3")
    folders = set()
    response = s3_client.list_objects_v2(Bucket="asf-core-data", Prefix=dir_name)

    for content in response.get("Contents", []):
        # print(content)
        folders.add(os.path.dirname(content["Key"]))

    if base_name_only:
        folders = [Path(f).name for f in folders]

    return sorted(folders)


def load_data(data_path="S3", file_path="", usecols=None, dtype=None, low_memory=False):

    if str(data_path) == "S3":
        loaded_data = load_s3_data(
            "asf-core-data",
            file_path,
            usecols=usecols,
            dtype=dtype,
            low_memory=low_memory,
        )
    else:
        file_path = data_path / file_path
        loaded_data = pd.read_csv(
            file_path, usecols=usecols, dtype=dtype, low_memory=low_memory
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

    print(dir_files)

    return dir_files


def load_s3_data(bucket_name, file_name, usecols=None, dtype=None, low_memory=False):
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
        )
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
