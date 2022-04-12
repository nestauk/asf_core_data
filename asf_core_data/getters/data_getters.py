"""
script to get and save data from s3 bucket and from files.
"""
#######################################################

import boto3
import os
from fnmatch import fnmatch
from urllib.request import urlretrieve
from zipfile import ZipFile
from asf_core_data import PROJECT_DIR
import pandas as pd
import logging
import pickle
import json

#######################################################

s3 = boto3.resource("s3")
logger = logging.getLogger(__name__)


def load_s3_data(s3, bucket_name, file_name):
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
            os.path.join("s3://" + bucket_name, file_name), encoding="unicode_escape"
        )
    else:
        print('Function not supported for file type other than "*.xlsx" and "*.csv"')


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
