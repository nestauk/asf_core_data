from asf_core_data import PROJECT_DIR, get_yaml_config, Path
from asf_core_data.config import base_config

import warnings
import boto3


def get_all_batch_names(data_path=None, rel_path=base_config.RAW_DATA_PATH):
    """_summary_

    Args:
        data_path (_type_, optional): _description_. Defaults to None.
        rel_path (_type_, optional): _description_. Defaults to base_config.RAW_DATA_PATH.

    Returns:
        _type_: _description_
    """

    if data_path == "S3":

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

        data_path = get_version_path(data_path / rel_path.parent, data_path=data_path)
        batches = [p.name for p in data_path.glob("*/") if not p.name.startswith(".")]

    return batches


def get_most_recent_batch(data_path=None, rel_path=base_config.RAW_DATA_PATH):

    if data_path == "S3":

        batches = get_all_batch_names(data_path="S3")

    else:
        batches = get_all_batch_names(data_path=data_path, rel_path=rel_path)

    return sorted(batches, reverse=True)[0]


def check_for_newest_batch(
    data_path=None, rel_path=base_config.RAW_DATA_PATH, verbose=False
):

    local_batch = get_most_recent_batch(data_path=data_path)
    s3_batch = get_most_recent_batch(data_path="S3")

    if local_batch == s3_batch:
        if verbose:
            print("Your local data is up to date with batch {}".format(local_batch))
        return (True, local_batch)

    else:
        if verbose:
            print(
                "With batch {} your local data is not up to date. The newest batch {} is available on S3.".format(
                    local_batch, s3_batch
                )
            )
        return (False, s3_batch)


def get_version_path(path, data_path, batch="newest"):

    if not "{}" in str(path):
        return path

    if batch is None or batch.lower() in [
        "newest",
        "most recent",
        "most_recent",
        "latest",
    ]:

        newest_batch = get_most_recent_batch(data_path=data_path)
        path = str(path).format(newest_batch)

        is_newest, newest_s3_batch = check_for_newest_batch(data_path=data_path)
        if not is_newest:
            warnings.warn(
                "You are loading the newest local batch - but a newer batch ({}) is available on S3.".format(
                    newest_s3_batch
                )
            )

    else:
        path = str(path).format(batch.upper())

    return Path(path)
