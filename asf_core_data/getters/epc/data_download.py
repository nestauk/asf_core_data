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


def download_s3_folder(s3_folder, local_dir):
    """
    Download the contents of a folder directory on the asf-core-data S3 bucket into a local directory.
    Args:
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


def download_core_data(dataset, local_dir, batch=None, unzip=True):
    """Download the ASF core data from the S3 bucket to local directory.

    Args:
        dataset (str): Which dataset to download.
            Options: epc_raw, epc_raw_combined, epc_preprocessed_dedupl, epc_preprocessed,
            EST_cleansed, EST_cleansed_dedupl, supplementary_data
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
        download_s3_folder(data_dict[dataset], local_dir)
        return
    else:
        data_to_load = str(data_dict[dataset]) + ".zip"

    s3_path = data_batches.get_batch_path(data_to_load, "S3", batch=batch)

    output_path = Path(local_dir) / s3_path

    Path(output_path.parent).mkdir(parents=True, exist_ok=True)
    data_getters.download_from_s3(str(s3_path), str(output_path))

    if unzip:
        with zipfile.ZipFile(output_path, "r") as zip_ref:
            zip_ref.extractall(output_path.parent)

    dirpath = Path(output_path.parent / "__MACOSX")

    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)


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


if __name__ == "__main__":
    # Execute only if run as a script
    main()
