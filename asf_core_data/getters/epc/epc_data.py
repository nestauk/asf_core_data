# File: getters/epc_data.py
"""Extracting and loading the EPC data."""

# ---------------------------------------------------------------------------------

from email.mime import base
from hashlib import new

# from multiprocessing import _RLockType
import os
import re
import random
import pandas as pd
from regex import D
import numpy as np
from zipfile import ZipFile


from asf_core_data import PROJECT_DIR, get_yaml_config, Path
from asf_core_data.config import base_config

import warnings

import boto3

# ---------------------------------------------------------------------------------


def get_all_batch_names(data_path=None, rel_path=base_config.RAW_DATA_PATH):

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


def load_england_wales_recommendations(
    data_path=base_config.ROOT_DATA_PATH,
    rel_data_path=base_config.RAW_ENG_WALES_DATA_PATH,
    batch=None,
    subset=None,
    usecols=None,
    n_samples=None,
    dtype=base_config.dtypes,
    low_memory=True,
):

    """Load the England and/or Wales EPC data.

    Parameters
    ----------
    subset : {'England', 'Wales', None}, default=None
        EPC certificate area subset.
        If None, then the data for both England and Wales will be loaded.

    usecols : list, default=None
        List of features/columns to load from EPC dataset.
        If None, then all features will be loaded.

    n_samples : int, default=None
        Number of rows of file to read.

    low_memory : bool, default=False
        Internally process the file in chunks, resulting in lower memory use while parsing,
        but possibly mixed type inference.
        To ensure no mixed types either set False, or specify the type with the dtype parameter.

    Return
    ---------
    EPC_certs : pandas.DateFrame
        England/Wales EPC certificate data for given features."""

    RAW_ENG_WALES_DATA_PATH = get_version_path(
        data_path / rel_data_path, data_path=data_path, batch=batch
    )
    RAW_ENG_WALES_DATA_ZIP = get_version_path(
        data_path / base_config.RAW_ENG_WALES_DATA_ZIP, data_path=data_path, batch=batch
    )

    # If sample file does not exist (probably just not unzipped), unzip the data
    if not (
        RAW_ENG_WALES_DATA_PATH / "domestic-W06000015-Cardiff/certificates.csv"
    ).is_file():
        extract_data(RAW_ENG_WALES_DATA_ZIP)

    # Get all directories
    directories = [
        dir
        for dir in os.listdir(RAW_ENG_WALES_DATA_PATH)
        if not (dir.startswith(".") or dir.endswith(".txt") or dir.endswith(".zip"))
    ]

    # Set subset dict to select respective subset directories
    start_with_dict = {"Wales": "domestic-W", "England": "domestic-E"}

    # Get directories for given subset
    if subset in start_with_dict:
        directories = [
            dir for dir in directories if dir.startswith(start_with_dict[subset])
        ]

    # Load EPC certificates for given subset
    # Only load columns of interest (if given)
    epc_certs = [
        pd.read_csv(
            RAW_ENG_WALES_DATA_PATH / directory / "recommendations.csv",
            dtype=dtype,
            low_memory=low_memory,
            usecols=usecols,
        )
        for directory in directories
    ]

    # Concatenate single dataframes into dataframe
    epc_certs = pd.concat(epc_certs, axis=0)
    epc_certs["COUNTRY"] = subset

    if n_samples is not None:
        epc_certs = epc_certs.sample(frac=1).reset_index(drop=True)[:n_samples]

    return epc_certs


def extract_data(file_path):
    """Extract data from zip file.

    Parameters
    ----------
    file_path : str
        Path to the file to unzip.

    Return: None"""

    # Check whether file exists
    if not Path(file_path).is_file():
        raise IOError("The file '{}' does not exist.".format(file_path))

    # Get directory
    zip_dir = file_path.parent

    # Unzip the data
    with ZipFile(file_path, "r") as zip:

        print("Extracting...\n{}".format(zip.filename))
        zip.extractall(zip_dir)
        print("Done!")


def load_scotland_data(
    data_path=base_config.ROOT_DATA_PATH,
    rel_data_path=base_config.RAW_SCOTLAND_DATA_PATH,
    batch=None,
    usecols=None,
    n_samples=None,
    dtype=base_config.dtypes,
    low_memory=True,
):
    """Load the Scotland EPC data.

    Parameters
    ----------
    usecols : list, default=None
        List of features/columns to load from EPC dataset.
        If None, then all features will be loaded.

    n_samples : int, default=None
        Number of rows of file to read.

    low_memory : bool, default=False
        Internally process the file in chunks, resulting in lower memory use while parsing,
        but possibly mixed type inference.
        To ensure no mixed types either set False, or specify the type with the dtype parameter.

    Return
    ---------
    EPC_certs : pandas.DateFrame
        Scotland EPC certificate data for given features."""

    RAW_SCOTLAND_DATA_PATH = get_version_path(
        Path(data_path) / rel_data_path, data_path=data_path, batch=batch
    )
    RAW_SCOTLAND_DATA_ZIP = get_version_path(
        Path(data_path) / base_config.RAW_ENG_WALES_DATA_ZIP,
        data_path=data_path,
        batch=batch,
    )

    # If sample file does not exist (probably just not unzipped), unzip the data
    if not [
        file
        for file in os.listdir(RAW_SCOTLAND_DATA_PATH)
        if file.startswith("D_EPC_data_")
    ]:
        extract_data(RAW_SCOTLAND_DATA_ZIP)

    if usecols is not None:
        # Fix columns ("WALLS" features are labeled differently here)
        usecols = [re.sub("WALLS_", "WALL_", col) for col in usecols]
        usecols = [re.sub("POSTTOWN", "POST_TOWN", col) for col in usecols]
        usecols = [col for col in usecols if col not in ["UPRN"]]

    # Get all directories
    all_directories = os.listdir(RAW_SCOTLAND_DATA_PATH)
    directories = [file for file in all_directories if file.endswith(".csv")]

    epc_certs = [
        pd.read_csv(
            RAW_SCOTLAND_DATA_PATH / file,
            dtype=dtype,
            low_memory=low_memory,
            usecols=usecols,
            skiprows=1,  # don't load first row (more ellaborate feature names),
            encoding="ISO-8859-1",
        )
        for file in directories
    ]

    # Concatenate single dataframes into dataframe
    epc_certs = pd.concat(epc_certs, axis=0)
    epc_certs["COUNTRY"] = "Scotland"

    epc_certs = epc_certs.rename(
        columns={
            "WALL_ENV_EFF": "WALLS_ENV_EFF",
            "WALL_ENERGY_EFF": "WALLS_ENERGY_EFF",
            "POST_TOWN": "POSTTOWN",
        }
    )

    if "UPRN" in epc_certs.columns:
        epc_certs["UPRN"] = epc_certs["BUILDING_REFERENCE_NUMBER"]
    if n_samples is not None:
        epc_certs = epc_certs.sample(frac=1).reset_index(drop=True)[:n_samples]

    return epc_certs


def load_england_wales_data(
    data_path=base_config.ROOT_DATA_PATH,
    rel_data_path=base_config.RAW_ENG_WALES_DATA_PATH,
    batch=None,
    subset=None,
    usecols=None,
    n_samples=None,
    dtype=base_config.dtypes,
    low_memory=True,
):
    """Load the England and/or Wales EPC data.

    Parameters
    ----------
    subset : {'England', 'Wales', None}, default=None
        EPC certificate area subset.
        If None, then the data for both England and Wales will be loaded.

    usecols : list, default=None
        List of features/columns to load from EPC dataset.
        If None, then all features will be loaded.

    n_samples : int, default=None
        Number of rows of file to read.

    low_memory : bool, default=False
        Internally process the file in chunks, resulting in lower memory use while parsing,
        but possibly mixed type inference.
        To ensure no mixed types either set False, or specify the type with the dtype parameter.

    Return
    ---------
    EPC_certs : pandas.DateFrame
        England/Wales EPC certificate data for given features."""

    if subset in [None, "GB", "all"]:

        additional_samples = 0

        if n_samples is not None:
            additional_samples = n_samples % 2
            n_samples = n_samples // 2

        wales_epc = load_england_wales_data(
            data_path=data_path,
            rel_data_path=rel_data_path,
            batch=batch,
            subset="Wales",
            usecols=usecols,
            dtype=dtype,
            low_memory=low_memory,
            n_samples=n_samples,
        )

        england_epc = load_england_wales_data(
            data_path=data_path,
            rel_data_path=rel_data_path,
            batch=batch,
            subset="England",
            usecols=usecols,
            n_samples=None if n_samples is None else n_samples + additional_samples,
            dtype=dtype,
            low_memory=low_memory,
        )

        epc_certs = pd.concat([wales_epc, england_epc], axis=0)

        return epc_certs

    RAW_ENG_WALES_DATA_PATH = get_version_path(
        Path(data_path) / rel_data_path, data_path=data_path, batch=batch
    )
    RAW_ENG_WALES_DATA_ZIP = get_version_path(
        Path(data_path) / base_config.RAW_ENG_WALES_DATA_ZIP,
        data_path=data_path,
        batch=batch,
    )

    # If sample file does not exist (probably just not unzipped), unzip the data
    if not Path(
        RAW_ENG_WALES_DATA_PATH / "domestic-W06000015-Cardiff/certificates.csv"
    ).is_file():
        extract_data(RAW_ENG_WALES_DATA_ZIP)

    # Get all directories
    directories = [
        dir
        for dir in os.listdir(RAW_ENG_WALES_DATA_PATH)
        if not (dir.startswith(".") or dir.endswith(".txt") or dir.endswith(".zip"))
    ]

    # Set subset dict to select respective subset directories
    start_with_dict = {"Wales": "domestic-W", "England": "domestic-E"}

    directories = [
        dir for dir in directories if dir.startswith(start_with_dict[subset])
    ]

    # Load EPC certificates for given subset
    # Only load columns of interest (if given)
    epc_certs = [
        pd.read_csv(
            RAW_ENG_WALES_DATA_PATH / directory / "certificates.csv",
            dtype=dtype,
            low_memory=low_memory,
            usecols=usecols,
            dtype=base_config.dtypes,
        )
        for directory in directories
    ]

    # Concatenate single dataframes into dataframe
    epc_certs = pd.concat(epc_certs, axis=0)
    epc_certs["COUNTRY"] = subset

    if "UPRN" in epc_certs.columns:
        epc_certs["UPRN"].fillna(epc_certs.BUILDING_REFERENCE_NUMBER, inplace=True)

    if n_samples is not None:
        epc_certs = epc_certs.sample(frac=1).reset_index(drop=True)[:n_samples]

    return epc_certs


def load_raw_epc_data(
    data_path=base_config.ROOT_DATA_PATH,
    rel_data_path=base_config.RAW_DATA_PATH,
    batch=None,
    subset="GB",
    usecols=None,
    n_samples=None,
    dtype=base_config.dtypes,
    low_memory=True,
):
    """Load and return EPC dataset, or specific subset, as pandas dataframe.

    Parameters
    ----------
    subset : {'GB', 'Wales', 'England', 'Scotland', None}, default='GB'
        EPC certificate area subset.

    usecols : list, default=None
        List of features/columns to load from EPC dataset.
        If None, then all features will be loaded.

    n_samples : int, default=None
        Number of rows of file to read.

    low_memory : bool, default=False
        Internally process the file in chunks, resulting in lower memory use while parsing,
        but possibly mixed type inference.
        To ensure no mixed types either set False, or specify the type with the dtype parameter.

    Return
    ---------
    EPC_certs : pandas.DateFrame
        EPC certificate data for given area and features."""

    RAW_DATA_PATH = get_version_path(Path(data_path), data_path=data_path, batch=batch)

    if rel_data_path != base_config.RAW_DATA_PATH:
        wales_england_path = RAW_DATA_PATH / "England_Wales"
        scotland_path = RAW_DATA_PATH / "Scotland"

    else:
        wales_england_path = base_config.RAW_ENG_WALES_DATA_PATH
        scotland_path = base_config.RAW_SCOTLAND_DATA_PATH

    all_epc_df = []
    additional_samples = 0

    if subset == "GB" and n_samples is not None:
        additional_samples = n_samples % 3
        n_samples = n_samples // 3

    # Get Scotland data
    if subset in ["Scotland", "GB"]:

        epc_Scotland_df = load_scotland_data(
            data_path=RAW_DATA_PATH,
            rel_data_path=scotland_path,
            usecols=usecols,
            n_samples=None if n_samples is None else n_samples + additional_samples,
        )

        all_epc_df.append(epc_Scotland_df)

        if subset == "Scotland":
            return epc_Scotland_df

    # Get the Wales/England data
    if subset in ["Wales", "England"]:
        epc_df = load_england_wales_data(
            data_path=RAW_DATA_PATH,
            rel_data_path=wales_england_path,
            subset=subset,
            usecols=usecols,
            n_samples=n_samples,
            dtype=dtype,
            low_memory=low_memory,
        )
        return epc_df

    # Merge the two datasets for GB
    elif subset == "GB":

        for country in ["Wales", "England"]:

            epc_df = load_england_wales_data(
                data_path=RAW_DATA_PATH,
                rel_data_path=wales_england_path,
                subset=country,
                usecols=usecols,
                n_samples=n_samples,
                dtype=dtype,
                low_memory=low_memory,
            )
            all_epc_df.append(epc_df)

        epc_df = pd.concat(all_epc_df, axis=0)

        return epc_df

    else:
        raise IOError("'{}' is not a valid subset of the EPC dataset.".format(subset))


def load_cleansed_epc(
    data_path=base_config.ROOT_DATA_PATH,
    rel_data_path=base_config.EST_CLEANSED_EPC_DATA_DEDUPL_PATH,
    remove_duplicates=True,
    usecols=None,
    n_samples=None,
):
    """Load the cleansed EPC dataset (provided by EST)
    with the option of excluding/including duplicates.

    Parameters
    ----------
    remove_duplicates : bool, default=True.
        Whether or not to remove duplicates.

    usecols : list, default=None
        List of features/columns to load from EPC dataset.
        If None, then all features will be loaded.

    nrows : int, default=None
        Number of rows of file to read.

    Return
    ----------
    cleansed_epc : pandas.DataFrame
        Cleansed EPC datast as dataframe."""

    EST_CLEANSED_PATH = data_path / rel_data_path

    if remove_duplicates:
        if (
            EST_CLEANSED_PATH
            != data_path / base_config.EST_CLEANSED_EPC_DATA_DEDUPL_PATH
        ):
            raise Warning(
                "Make sure you passed the path to the deduplicated EST cleansed version, as it cannot be confirmed when passing custom paths."
            )

        if EST_CLEANSED_PATH == data_path / base_config.EST_CLEANSED_EPC_DATA_PATH:
            EST_CLEANSED_PATH = (
                data_path / base_config.EST_CLEANSED_EPC_DATA_DEDUPL_PATH
            )
            raise Warning(
                "Path was corrected from {} to {} since remove_duplicates is set to True.".format(
                    base_config.EST_CLEANSED_EPC_DATA_PATH,
                    base_config.EST_CLEANSED_EPC_DATA_DEDUPL_PATH,
                )
            )

    else:
        if (
            EST_CLEANSED_PATH
            == data_path / base_config.EST_CLEANSED_EPC_DATA_DEDUPL_PATH
        ):
            EST_CLEANSED_PATH = data_path / base_config.EST_CLEANSED_EPC_DATA_PATH
            raise Warning(
                "Path was corrected from {} to {} since remove_duplicates is set to False.".format(
                    base_config.EST_CLEANSED_EPC_DATA_DEDUPL_PATH,
                    base_config.EST_CLEANSED_EPC_DATA_PATH,
                )
            )

    # If file does not exist (probably just not unzipped), unzip the data
    if not EST_CLEANSED_PATH.is_file():
        extract_data(EST_CLEANSED_PATH / ".zip")

    print("Loading cleansed EPC data... This will take a moment.")
    cleansed_epc = pd.read_csv(
        EST_CLEANSED_PATH, usecols=usecols, nrows=n_samples, low_memory=False
    )

    # Drop first column
    if "Unnamed: 0" in cleansed_epc.columns:
        cleansed_epc = cleansed_epc.drop(columns="Unnamed: 0")

    # Add HP feature
    cleansed_epc["HEAT_PUMP"] = cleansed_epc.FINAL_HEATING_SYSTEM == "Heat pump"

    return cleansed_epc


def load_preprocessed_epc_data(
    data_path=base_config.ROOT_DATA_PATH,
    rel_data_path=base_config.RAW_EPC_DATA_PATH.parent,
    batch=None,
    version="preprocessed_dedupl",
    usecols=None,
    n_samples=None,
    snapshot_data=False,
    dtype=base_config.dtypes,
    low_memory=True,
):
    """Load the EPC dataset including England, Wales and Scotland.
    Select one of the following versions:

        - raw:
        EPC data merged for all countries but otherwise not altered

        - preprocessed:
        Partially cleaned and with additional features

        - preprocessed_dedupl:
        Same as 'preprocessed' but without duplicates

    Parameters
    ----------
    version : str, {'raw', 'preprocessed', 'preprocessed_dedupl'}, default='preprocessed_dedupl'
        The version of the EPC data to load.

    usecols : list, default=None
        List of features/columns to load from EPC dataset.
        If None, then all features will be loaded.

    n_samples : int, default=None
        Number of rows of file to read.

    snapshot_data : bool, default=False
        If True, load the snapshot version of the preprocessed EPC data saved in /inputs
        instead of the most recent version in /outputs.

    low_memory : bool, default=False
        Internally process the file in chunks, resulting in lower memory use while parsing,
        but possibly mixed type inference.
        To ensure no mixed types either set False, or specify the type with the dtype parameter.

    Return
    ----------
    epc_df : pandas.DataFrame
        EPC data in the given version."""

    version_path_dict = {
        "raw": base_config.RAW_EPC_DATA_PATH.name,
        "preprocessed_dedupl": base_config.PREPROC_EPC_DATA_DEDUPL_PATH.name,
        "preprocessed": base_config.PREPROC_EPC_DATA_PATH.name,
    }

    version_path_snapshot_dict = {
        "raw": base_config.SNAPSHOT_RAW_EPC_DATA_PATH.name,
        "preprocessed_dedupl": base_config.SNAPSHOT_PREPROC_EPC_DATA_DEDUPL_PATH.name,
        "preprocessed": base_config.SNAPSHOT_PREPROC_EPC_DATA_PATH.name,
    }

    EPC_DATA_PATH = get_version_path(
        Path(data_path) / rel_data_path / version_path_dict[version],
        data_path=data_path,
        batch=batch,
    )

    if snapshot_data:
        EPC_DATA_PATH = (
            Path(data_path) / rel_data_path / version_path_snapshot_dict[version]
        )

    # If file does not exist (likely just not unzipped), unzip the data
    if not EPC_DATA_PATH.is_file():
        extract_data(EPC_DATA_PATH.parent / (EPC_DATA_PATH.name + ".zip"))

    # Load  data
    epc_df = pd.read_csv(
        EPC_DATA_PATH,
        usecols=usecols,
        nrows=n_samples,
        dtype=dtype,
        low_memory=low_memory,
    )

    for col in base_config.parse_dates:
        if col in epc_df.columns:
            epc_df[col] = pd.to_datetime(epc_df[col])

    return epc_df


def get_epc_sample(full_df, sample_size):
    """Randomly sample a subset of the full data.

    Parameters
    ----------
    full_df : pandas.DataFrame
        Full dataframe from which to extract a subset.

    sample_size: int
        Size of subset / number of samples.

    Return
    ----------
    sample_df : pandas.DataFrame
        Randomly sampled subset of full dataframe."""

    rand_ints = np.random.choice(len(full_df), size=sample_size)
    sample_df = full_df.iloc[rand_ints]

    return sample_df


def main():
    """Main function for testing."""

    epc = load_preprocessed_epc_data(version="preprocessed_dedupl")


if __name__ == "__main__":
    # Execute only if run as a script
    main()
