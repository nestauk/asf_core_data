# File: asf_core_data/getters/epc/data_batches.py
"""Look up and integrate batch names."""

# ---------------------------------------------------------------------------------

# Imports
from asf_core_data import Path
from asf_core_data.config import base_config
from asf_core_data.getters import data_getters

import warnings
import boto3

# ---------------------------------------------------------------------------------


def get_batch_path(rel_path, data_path, batch="newest"):
    """Create path to specific batch, e.g. to the newest batch.

    Args:
        path (str/Path): Path that needs to be updated with batch name.
        data_path (str/Path, optional): Path to ASF core data directory. Defaults to None.
        batch (str, optional): Which batch to use, either specific batch name or relative indicator. Defaults to "newest".

    Returns:
        Path: Path with specific batch name integrated.
    """

    # If batch does not need to be filled in
    if not "{}" in str(rel_path):
        return Path(rel_path)

    # Get most recent batch
    if batch is None or batch.lower() in [
        "newest",
        "most recent",
        "most_recent",
        "latest",
    ]:

        newest_batch = get_most_recent_batch(data_path=data_path)
        path = str(rel_path).format(newest_batch)

        # Warn if not the newest batch
        if data_path != "S3":
            is_newest, newest_s3_batch = check_for_newest_batch(data_path=data_path)
            if not is_newest:
                warnings.warn(
                    "You are loading the newest local batch - but a newer batch ({}) is available on S3.".format(
                        newest_s3_batch
                    )
                )

    else:
        path = str(rel_path).format(batch.upper().replace("COMPLETE", "complete"))

    return Path(path)


def get_all_batch_names(
    data_path="S3", rel_path=base_config.RAW_DATA_PATH, check_folder="input"
):
    """Get all batch names for EPC versions stored on the S3 bucket 'asf-core-data'
    or in a specific directory.

    Args:
        data_path (str/Path, optional): Path to ASF core data directory or 'S3'. Defaults to None.
        rel_path (str/Path, optional): Relative path to EPC data. Defaults to base_config.RAW_DATA_PATH.
        check_folder (str, optional): Whether to check in the input or output folder. Options: input(s), output(s).

    Returns:
        list: All EPC batches.
    """

    if check_folder not in ["input", "inputs", "output", "outputs"]:
        raise IOError(
            'The value for check_folder has to be "input", "inputs", "output" or "outputs".'
        )

    if check_folder in ["output", "outputs"]:
        rel_path = base_config.OUTPUT_DATA_PATH
    else:
        rel_path = rel_path.parent

    if str(data_path) == "S3":
        client = boto3.client("s3")
        bucket = "asf-core-data"
        path = "inputs/EPC/raw_data/"

        batches = [
            Path(obj["Key"]).stem
            for obj in client.list_objects(Bucket=bucket, Prefix=path, Delimiter="/")[
                "Contents"
            ]
            if obj["Key"].endswith(".zip")
        ]

    else:

        data_path = get_batch_path(data_path / rel_path, data_path=data_path)
        batches = [
            p.name
            for p in data_path.glob("*/")
            if not p.name.startswith(".")
            and not p.name.endswith(".zip")
            and not p.name.startswith("__")
        ]

    return batches


def get_latest_mcs_epc_joined_batch(version="most_relevant"):
    """Get the filename for the latest batch of MCS-EPC joined data for given version.

    Args:
        version (str, optional): Which merging type is used for EPC. Defaults to most_relevant.
            All options: most_relevant, newest, full.

    Returns:
        str: Latest EPC/MCS batch filename for given version.
    """

    s3 = boto3.resource("s3")
    bucket = "asf-core-data"
    path = "outputs/MCS/"

    batches = [
        key for key in data_getters.get_s3_dir_files(bucket, path) if "old" not in key
    ]
    batches = [batch for batch in batches if batch[:-11].endswith(version)]

    if not batches:
        raise IOError("No batch suitable baches found.")

    # Return highest value since files are marked with date stamps in format yyyymmdd
    return max(batches)


def get_most_recent_batch(
    data_path=None, rel_path=base_config.RAW_DATA_PATH, check_folder="input"
):
    """Get the most recent EPC data batch from ASF data directory or S3 bucket.

    Args:
        data_path (str/Path, optional): Path to ASF core data directory or 'S3'. Defaults to None.
        rel_path (str/Path, optional): Relative path to EPC data. Defaults to base_config.RAW_DATA_PATH.
        check_folder (str, optional): Whether to check in the input or output folder. Options: input(s), output(s).

    Returns:
        str: Most recent batch name.
    """

    if str(data_path) == "S3":

        batches = get_all_batch_names(data_path="S3")

    else:
        batches = get_all_batch_names(
            data_path=data_path, rel_path=rel_path, check_folder=check_folder
        )

    if not batches:
        raise IOError("No batch found in {}.".format(data_path))

    return max(batches)


def check_for_newest_batch(data_path=None, check_folder="input", verbose=False):
    """Check whether the local data dir is up-to-date and includes the newest EPC data batch.

    Args:
        data_path (str/Path, optional): Path to ASF core data directory. Defaults to None.
        check_folder (str, optional): Whether to check in the input or output folder. Options: input(s), output(s).
        verbose (bool, optional): Whether to print results. Defaults to True.

    Returns:
        (bool, str): Whether or not local dir is up-to-date, newest batch name.
    """

    # Get the latest batches on the local dir and S3
    local_batch = get_most_recent_batch(data_path=data_path, check_folder=check_folder)
    s3_batch = get_most_recent_batch(data_path="S3")

    if local_batch == s3_batch:
        if verbose:
            print("Your local data is up to date with batch {}".format(local_batch))
        return (True, s3_batch)

    else:
        if verbose:
            print(
                "With batch {} your local data is not up to date. The newest batch {} is available on S3.".format(
                    local_batch, s3_batch
                )
            )
        return (False, s3_batch)
