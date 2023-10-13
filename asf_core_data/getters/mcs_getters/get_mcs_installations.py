# File: asf_core_data/getters/mcs/get_mcs_installations.py
"""Function to get concatenated installations data in a usable format,
with correct dtypes."""

import pandas as pd
import os
import re

from asf_core_data.getters.data_getters import (
    s3,
    load_s3_data,
    get_most_recent_batch_name,
    logger,
)

from asf_core_data.config import base_config

bucket_name = base_config.BUCKET_NAME
mcs_processed_dir = base_config.MCS_PROCESSED_FILES_PATH
mcs_installations_path = base_config.MCS_INSTALLATIONS_PATH
mcs_installations_epc_full_path = base_config.MCS_INSTALLATIONS_EPC_FULL_PATH
mcs_installations_epc_most_relevant_path = (
    base_config.MCS_INSTALLATIONS_EPC_MOST_RELEVANT_PATH
)

keyword_to_path_dict = {
    "none": mcs_installations_path,
    "full": mcs_installations_epc_full_path,
    "most_relevant": mcs_installations_epc_most_relevant_path,
}


def get_raw_installations_data(
    local_path=base_config.ROOT_DATA_PATH + base_config.INSTALLATIONS_RAW_LOCAL_PATH,
    refresh=True,
    verbose=True,
):
    """Get raw MCS HP installation data (both domestic and non-domestic)
    with no processing other than renaming columns and setting correct dtypes.

    Args:
        local_path (str, optional): Local path to raw data.
            Defaults to str(PROJECT_DIR) + INSTALLATIONS_RAW_LOCAL_PATH.
        refresh (bool, optional): Whether or not to update the local copy
            of the data from S3. If local_path does not exist, data will be
            pulled from S3 even if refresh is False. Defaults to True.
        verbose (bool, optional): Whether to print info about the number of
            samples in loaded data.

    Returns:
        DataFrame: Raw HP installation data.
    """

    if refresh or not os.path.exists(local_path):
        hps = load_s3_data(
            bucket_name, base_config.INSTALLATIONS_RAW_S3_PATH, usecols=None
        )
    else:
        hps = pd.read_excel(local_path)

    hps = hps.astype(
        {
            "address_1": str,
            "address_2": str,
            "address_3": str,
            "postcode": str,
        }
    )
    hps["commission_date"] = pd.to_datetime(hps["commission_date"])
    hps = (
        hps.convert_dtypes()
        .drop_duplicates(
            subset=[
                "version",
                "commission_date",
                "address_1",
                "address_2",
                "address_3",
                "postcode",
            ]
        )
        .reset_index(drop=True)
    )

    if verbose:
        print("Shape of loaded data:", hps.shape)

    return hps


def get_most_recent_raw_historical_installations_data() -> pd.DataFrame:
    """
    Get the most recent version of the raw historical heat pump MCS certified installation data from S3.

    Returns:
        The most recent version of the raw historical installations data from MCS.
    """
    path = base_config.MCS_HISTORICAL_DATA_INPUTS_PATH
    most_recent_file_name = get_most_recent_batch_name(
        bucket=bucket_name, s3_folder_path=path, filter_keep_keywords=["installation"]
    )

    return load_s3_data(
        bucket_name,
        most_recent_file_name,
        dtype=base_config.raw_historical_installations_dtypes,
    )


def get_raw_historical_installations_data(
    raw_historical_installations_file_name: str,
) -> pd.DataFrame:
    """
    Get a specified version of the raw MCS historical installation data (both domestic and non-domestic)
    for S3 bucket.

    Args:
        raw_historical_installations_file_name: name of file in S3 bucket, raw_historical_installations_YYYYMMDD.xlsx
        where YYYY/MM/DD is the date MCS shared the file in the Data Dumps Google Drive folder.
    Returns:
        Raw historical installation data
    """

    return load_s3_data(
        bucket_name,
        os.path.join(
            base_config.MCS_HISTORICAL_DATA_INPUTS_PATH,
            raw_historical_installations_file_name,
        ),
        dtype=base_config.raw_historical_installations_dtypes,
    )


def get_processed_installations_data(
    processed_installations_file_name: str,
) -> pd.DataFrame:
    """
    Get a specified version of the processed MCS installation data (both domestic and non-domestic)
    from S3 (either just MCS or MCS merged with EPC) using filename.

    Args:
        processed_installations_file_name: name of processed file
    Returns:
        Processed installations data (either merged or not merged with EPC)
    """

    return load_s3_data(
        bucket_name,
        os.path.join(
            base_config.MCS_PROCESSED_FILES_PATH,
            processed_installations_file_name,
        ),
    )


def find_most_recent_mcs_installations_batch(epc_version: str = "none") -> str:
    """
    Finds most recent batch of MCS installations data, joined with EPC data in specified format if specified.
    Args:
        epc_version: One of "none", "full", or "most_relevant".
            - "none" returns just installation data
            - "full" returns installation data with each property's entire EPC history attached
            - "most_relevant" selects the most recent EPC from before the HP installation if one exists or the earliest
            EPC from after the HP installation otherwise. Defaults to "none".
    Returns:
        str: Filename of most recent MCS installations (+ EPC) data.
    """
    bucket = s3.Bucket(bucket_name)
    file_list = [
        object.key for object in bucket.objects.filter(Prefix=mcs_processed_dir)
    ]
    file_prefix = keyword_to_path_dict[epc_version].split("{")[0]
    matches = [
        filename
        for filename in file_list
        if ("/" + re.split("[0-9]", filename)[0] == file_prefix)
    ]
    try:
        latest_version = max(matches)
        logger.info(f"Latest version available on S3: <{latest_version}>")
        return latest_version
    except ValueError:
        logger.error(
            f"ValueError: No files found in {bucket} bucket for epc_version='{epc_version}'"
        )


def get_processed_installations_data_by_batch(
    batch_date: str = "newest", epc_version: str = "none"
) -> pd.DataFrame:
    """
    Get a specified version of the processed MCS installation (+ EPC) data (both domestic and non-domestic)
    from S3 (either just MCS or MCS merged with EPC) using batch date.

    Args:
        batch_date: Date of desired batch of processed MCS installation data in YYMMDD format. Defaults to "newest"
        which searches S3 for latest batch.
        epc_version: One of "none", "full", or "most_relevant".
            - "none" returns just installation data
            - "full" returns installation data with each property's entire EPC history attached
            - "most_relevant" selects the most recent EPC from before the HP installation if one exists or the earliest
            EPC from after the HP installation otherwise. Defaults to "none".
    Returns:
        DataFrame: Processed MCS installations data (either merged or not merged with EPC)
    """
    if batch_date == "newest":
        processed_installations_file_path = find_most_recent_mcs_installations_batch(
            epc_version=epc_version
        )
    else:
        file_prefix = keyword_to_path_dict[epc_version]
        # note that [1:] removes the first "/" in the file path
        processed_installations_file_path = file_prefix.format(batch_date)[1:]
    logger.info(f"Loading <{processed_installations_file_path}> from S3")

    return load_s3_data(
        bucket_name=bucket_name, file_name=processed_installations_file_path
    )
