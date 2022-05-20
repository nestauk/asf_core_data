"""data getter functions to load and save data via s3.
"""
# To be reviewed in context of a different review!

#####################################################################
import os
import pickle
from fnmatch import fnmatch
import boto3


import pandas as pd


from asf_core_data import get_yaml_config, _base_config_path
from asf_core_data import Path

#####################################################################

s3 = boto3.resource("s3")
# get config file with relevant paramenters
config_info = get_yaml_config(_base_config_path)


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


def load_s3_data(bucket_name, file_name, usecols=True):
    """
    Load data from S3 location.
    s3: S3 boto3 resource
    bucket_name: The S3 bucket name
    file_name: S3 key to load
    """

    if fnmatch(file_name, "*.xlsx"):
        return pd.read_excel(os.path.join("s3://" + bucket_name, file_name))
    elif fnmatch(file_name, "*.csv"):
        return pd.read_csv(
            os.path.join("s3://" + bucket_name, file_name),
            encoding="latin-1",
            usecols=usecols,
        )
    elif fnmatch(file_name, "*.pickle") or fnmatch(file_name, "*.pkl"):
        obj = s3.Object(bucket_name, file_name)
        file = obj.get()["Body"].read()
        return pickle.loads(file)

    else:
        print(
            'Function not supported for file type other than "*.xlsx", "*.pickle", and "*.csv"'
        )


def save_to_s3(s3, bucket_name, output_var, output_file_dir):

    obj = s3.Object(bucket_name, output_file_dir)

    if fnmatch(output_file_dir, "*.pkl") or fnmatch(output_file_dir, "*.pickle"):
        byte_obj = pickle.dumps(output_var)
        obj.put(Body=byte_obj)
    elif fnmatch(output_file_dir, "*.csv"):
        output_var.to_csv("s3://" + bucket_name + output_file_dir, index=False)
    else:
        byte_obj = json.dumps(output_var)
    obj.put(Body=byte_obj)
