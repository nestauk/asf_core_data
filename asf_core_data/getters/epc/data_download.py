# File: asf_core_data/getters/epc/epc_data.py
"""Extracting and loading the EPC data."""

# ---------------------------------------------------------------------------------

from email.contentmanager import raw_data_manager
import os
import re
from tabnanny import check
from this import d
from typing import final
from zipfile import ZipFile
import shutil

import pandas as pd
import numpy as np

from asf_core_data import Path
from asf_core_data.getters.epc import data_batches
from asf_core_data.config import base_config

from asf_core_data.getters import data_getters

import zipfile

# ---------------------------------------------------------------------------------


data_dict = {
    "EPC_England/Wales": base_config.RAW_DATA_PATH,
    "EPC_Scotland": base_config.RAW_DATA_PATH,
    "raw": base_config.RAW_EPC_DATA_PATH,
    "preprocessed_dedupl": base_config.PREPROC_EPC_DATA_DEDUPL_PATH,
    "preprocessed": base_config.PREPROC_EPC_DATA_PATH,
    "EST_cleansed": base_config.EST_CLEANSED_EPC_DATA_PATH,
    "EST_cleansed_dedupl": base_config.EST_CLEANSED_EPC_DATA_DEDUPL_PATH,
    "supplementary_data": base_config.SUPPL_DATA_PATH,
}


import boto3
import os


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
    data_getters.download_from_s3(str(s3_path), str(output_path))

    if unzip:
        with zipfile.ZipFile(output_path, "r") as zip_ref:
            zip_ref.extractall(output_path.parent)

    dirpath = Path(output_path.parent / "__MACOSX")

    print(dirpath)
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)


if __name__ == "__main__":
    # Execute only if run as a script
    main()
