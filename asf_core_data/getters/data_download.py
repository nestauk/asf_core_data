# File: asf_core_data/getters/epc/epc_data.py
"""Extracting and loading the EPC data."""

# ---------------------------------------------------------------------------------


import shutil
import boto3
import os
import zipfile

from asf_core_data import Path
from asf_core_data.getters.epc import data_batches
from asf_core_data.config import base_config
from asf_core_data.getters import data_getters


# ---------------------------------------------------------------------------------


def download_s3_folder(s3_folder, local_dir, bucket_name="asf-core-data"):
    """
    Download the contents of a folder directory on the asf-core-data S3 bucket into a local directory.
    Args:
        s3_folder: the folder path in the s3 bucket
        local_dir: a relative or absolute directory path in the local file system
    """

    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=s3_folder):

        target = os.path.join(local_dir, s3_folder, os.path.relpath(obj.key, s3_folder))
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        if obj.key[-1] == "/":
            continue
        bucket.download_file(obj.key, target)


def extract_data(file_path):
    """Extract data from zip file.

    Args:
        file_path (str): Path to the file to unzip.

    Returns:
        None
    """

    # Check whether file exists
    if not Path(file_path).is_file():
        raise IOError("The file '{}' does not exist.".format(file_path))

    # Get directory
    zip_dir = file_path.parent

    # Unzip the data
    with zipfile.ZipFile(file_path, "r") as zip:

        print("Extracting...\n{}".format(zip.filename))
        zip.extractall(zip_dir)
        print("Done!")
