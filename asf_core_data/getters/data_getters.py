import boto3
import os
from fnmatch import fnmatch
import pandas as pd
import logging
import pickle
import json

s3 = boto3.resource("s3")
logger = logging.getLogger(__name__)


def get_s3_dir_files(s3, bucket_name, dir_name):
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

    return dir_files


def load_s3_data(
    bucket_name,
    file_name,
    usecols=None,
    dtypes=None,
    columns_to_parse_as_dates=None,
    encoding="latin-1",
):
    """
    Load data from S3 location.
    bucket_name: The S3 bucket name
    file_name: S3 key to load
    usecols: Columns of data to use. Defaults to None, loading all columns.
    dtypes: data types
    columns_to_parse_as_dates: columns that should be parsed as dates (when reading as csv)
    encoding: enconding when reading as csv (defaults to latin-1)
    """
    if fnmatch(file_name, "*.xlsx"):
        data = pd.read_excel(
            os.path.join("s3://" + bucket_name, file_name),
            sheet_name=None,
            dtype=dtypes,
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
            dtype=dtypes,
            parse_dates=columns_to_parse_as_dates,
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
    batches = [key for key in get_s3_dir_files(s3, bucket, s3_folder_path)]
    print(batches)

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
