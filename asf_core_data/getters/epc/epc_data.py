# File: getters/epc_data.py
"""Extracting and loading the EPC data."""

# ---------------------------------------------------------------------------------

import os
import re
import pandas as pd
import numpy as np
from zipfile import ZipFile


from asf_core_data import PROJECT_DIR, get_yaml_config, Path

# ---------------------------------------------------------------------------------

# Load config file
config = get_yaml_config(Path(str(PROJECT_DIR) + "/asf_core_data/config/base.yaml"))

print(PROJECT_DIR)

# Get paths
RAW_ENG_WALES_DATA_PATH = str(PROJECT_DIR) + config["RAW_ENG_WALES_DATA_PATH"]
RAW_SCOTLAND_DATA_PATH = str(PROJECT_DIR) + config["RAW_SCOTLAND_DATA_PATH"]

RAW_ENG_WALES_DATA_ZIP = str(PROJECT_DIR) + config["RAW_ENG_WALES_DATA_ZIP"]
RAW_SCOTLAND_DATA_ZIP = str(PROJECT_DIR) + config["RAW_SCOTLAND_DATA_ZIP"]

RAW_EPC_DATA_PATH = str(PROJECT_DIR) + config["RAW_EPC_DATA_PATH"]


def load_wales_certificates(subset=None, usecols=None, nrows=None, low_memory=False):

    """Load the England and/or Wales EPC data.

    Parameters
    ----------
    subset : {'England', 'Wales', None}, default=None
        EPC certificate area subset.
        If None, then the data for both England and Wales will be loaded.

    usecols : list, default=None
        List of features/columns to load from EPC dataset.
        If None, then all features will be loaded.

    nrows : int, default=None
        Number of rows of file to read.

    low_memory : bool, default=False
        Internally process the file in chunks, resulting in lower memory use while parsing,
        but possibly mixed type inference.
        To ensure no mixed types either set False, or specify the type with the dtype parameter.

    Return
    ---------
    EPC_certs : pandas.DateFrame
        England/Wales EPC certificate data for given features."""

    # If sample file does not exist (probably just not unzipped), unzip the data
    if not Path(
        RAW_ENG_WALES_DATA_PATH + "domestic-W06000015-Cardiff/certificates.csv"
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
            RAW_ENG_WALES_DATA_PATH + directory + "/recommendations.csv",
            low_memory=low_memory,
            usecols=usecols,
            nrows=nrows,
        )
        for directory in directories
    ]

    # Concatenate single dataframes into dataframe
    epc_certs = pd.concat(epc_certs, axis=0)
    epc_certs["COUNTRY"] = subset

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
    zip_dir = os.path.dirname(file_path) + "/"

    # Unzip the data
    with ZipFile(file_path, "r") as zip:

        print("Extracting...\n{}".format(zip.filename))
        zip.extractall(zip_dir)
        print("Done!")


def load_scotland_data(usecols=None, nrows=None, low_memory=False):
    """Load the Scotland EPC data.

    Parameters
    ----------
    usecols : list, default=None
        List of features/columns to load from EPC dataset.
        If None, then all features will be loaded.

    nrows : int, default=None
        Number of rows of file to read.

    low_memory : bool, default=False
        Internally process the file in chunks, resulting in lower memory use while parsing,
        but possibly mixed type inference.
        To ensure no mixed types either set False, or specify the type with the dtype parameter.

    Return
    ---------
    EPC_certs : pandas.DateFrame
        Scotland EPC certificate data for given features."""

    # If sample file does not exist (probably just not unzipped), unzip the data
    if not [
        file
        for file in os.listdir(RAW_SCOTLAND_DATA_PATH)
        if file.startswith("D_EPC_data_2012_Q4_extract")
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
            RAW_SCOTLAND_DATA_PATH + file,
            low_memory=low_memory,
            usecols=usecols,
            nrows=nrows,
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

    epc_certs["UPRN"] = epc_certs["BUILDING_REFERENCE_NUMBER"]

    return epc_certs


def load_wales_england_data(subset=None, usecols=None, nrows=None, low_memory=False):
    """Load the England and/or Wales EPC data.

    Parameters
    ----------
    subset : {'England', 'Wales', None}, default=None
        EPC certificate area subset.
        If None, then the data for both England and Wales will be loaded.

    usecols : list, default=None
        List of features/columns to load from EPC dataset.
        If None, then all features will be loaded.

    nrows : int, default=None
        Number of rows of file to read.

    low_memory : bool, default=False
        Internally process the file in chunks, resulting in lower memory use while parsing,
        but possibly mixed type inference.
        To ensure no mixed types either set False, or specify the type with the dtype parameter.

    Return
    ---------
    EPC_certs : pandas.DateFrame
        England/Wales EPC certificate data for given features."""

    # If sample file does not exist (probably just not unzipped), unzip the data
    if not Path(
        RAW_ENG_WALES_DATA_PATH + "domestic-W06000015-Cardiff/certificates.csv"
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
            RAW_ENG_WALES_DATA_PATH + directory + "/certificates.csv",
            low_memory=low_memory,
            usecols=usecols,
            nrows=nrows,
        )
        for directory in directories
    ]

    # Concatenate single dataframes into dataframe
    epc_certs = pd.concat(epc_certs, axis=0)
    epc_certs["COUNTRY"] = subset

    epc_certs["UPRN"].fillna(epc_certs.BUILDING_REFERENCE_NUMBER, inplace=True)

    return epc_certs


def load_raw_epc_data(subset="GB", usecols=None, nrows=None, low_memory=False):
    """Load and return EPC dataset, or specific subset, as pandas dataframe.

    Parameters
    ----------
    subset : {'GB', 'Wales', 'England', 'Scotland', None}, default='GB'
        EPC certificate area subset.

    usecols : list, default=None
        List of features/columns to load from EPC dataset.
        If None, then all features will be loaded.

    nrows : int, default=None
        Number of rows of file to read.

    low_memory : bool, default=False
        Internally process the file in chunks, resulting in lower memory use while parsing,
        but possibly mixed type inference.
        To ensure no mixed types either set False, or specify the type with the dtype parameter.

    Return
    ---------
    EPC_certs : pandas.DateFrame
        EPC certificate data for given area and features."""

    all_epc_df = []

    # Get Scotland data
    if subset in ["Scotland", "GB"]:
        epc_Scotland_df = load_scotland_data(usecols=usecols, nrows=nrows)
        all_epc_df.append(epc_Scotland_df)

        if subset == "Scotland":
            return epc_Scotland_df

    # Get the Wales/England data
    if subset in ["Wales", "England"]:
        epc_df = load_wales_england_data(
            subset, usecols=usecols, nrows=nrows, low_memory=low_memory
        )
        return epc_df

    # Merge the two datasets for GB
    elif subset == "GB":

        for country in ["Wales", "England"]:

            epc_df = load_wales_england_data(
                country, usecols=usecols, nrows=nrows, low_memory=low_memory
            )
            all_epc_df.append(epc_df)

        epc_df = pd.concat(all_epc_df, axis=0)

        return epc_df

    else:
        raise IOError("'{}' is not a valid subset of the EPC dataset.".format(subset))


def load_cleansed_epc(remove_duplicates=True, usecols=None, nrows=None):
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

    if remove_duplicates:
        file_path = str(PROJECT_DIR) + config["EST_CLEANSED_EPC_DATA_DEDUPL_PATH"]
    else:
        file_path = str(PROJECT_DIR) + config["EST_CLEANSED_EPC_DATA_PATH"]

    # If file does not exist (probably just not unzipped), unzip the data
    if not Path(file_path).is_file():
        extract_data(file_path + ".zip")

    print("Loading cleansed EPC data... This will take a moment.")
    cleansed_epc = pd.read_csv(
        file_path, usecols=usecols, nrows=nrows, low_memory=False
    )

    # Drop first column
    if "Unnamed: 0" in cleansed_epc.columns:
        cleansed_epc = cleansed_epc.drop(columns="Unnamed: 0")

    # Add HP feature
    cleansed_epc["HEAT_PUMP"] = cleansed_epc.FINAL_HEATING_SYSTEM == "Heat pump"
    print("Done!")

    return cleansed_epc


def load_preprocessed_epc_data(
    version="preprocessed_dedupl",
    usecols=None,
    nrows=None,
    snapshot_data=False,
    dtype={},
    low_memory=False,
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

    nrows : int, default=None
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
        "raw": "RAW_EPC_DATA_PATH",
        "preprocessed_dedupl": "PREPROC_EPC_DATA_DEDUPL_PATH",
        "preprocessed": "PREPROC_EPC_DATA_PATH",
    }

    # Get the respective file path for version
    file_path = str(PROJECT_DIR) + config[version_path_dict[version]]

    if snapshot_data:
        file_path = str(PROJECT_DIR) + config["SNAPSHOT_" + version_path_dict[version]]

    # If file does not exist (likely just not unzipped), unzip the data
    if not Path(file_path).is_file():
        extract_data(file_path + ".zip")

    # Load  data
    epc_df = pd.read_csv(
        file_path,
        usecols=usecols,
        nrows=nrows,
        dtype=dtype,
    )  # , low_memory=low_memory)

    for col in config["parse_dates"]:
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


if __name__ == "__main__":
    # Execute only if run as a script
    main()
