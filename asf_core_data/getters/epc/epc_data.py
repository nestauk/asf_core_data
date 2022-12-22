# File: asf_core_data/getters/epc/epc_data.py
"""Extracting and loading the EPC data."""

# ---------------------------------------------------------------------------------

import os
import re

import pandas as pd
import numpy as np
import copy

from asf_core_data import Path
from asf_core_data.getters.epc import data_batches
from asf_core_data.config import base_config
from asf_core_data.getters.epc import data_download

from asf_core_data.getters import data_getters

# ---------------------------------------------------------------------------------


# Set subset dict to select respective subset directories
start_with_dict = {"Wales": "domestic-W", "England": "domestic-E"}


def get_cert_rec_files(data_path, dir_name, scotland_data=False):
    """Get set of EPC certification and recommendation directories or files in given directory.
    For Scotland, the files storing the EPC records per quarter are returned.
    For England/Wales, the directories in which the EPC data is stored for each area is returned.

    Args:
        data_path(str/Path): Path to ASF core data directory or 'S3'.
        dir_name (str/Path): Path to directory where EPC data is stored.
        scotland_data (bool, optional): Whether or not Scotland data is loaded. Defaults to False.

    Returns:
        directories: Directory names/filenames where EPC data is stored for different areas/quarters.
    """

    if str(data_path) == "S3":

        if scotland_data:

            directories = [
                Path(f).name
                for f in data_getters.get_s3_dir_files(
                    dir_name=str(dir_name), direct_child_only=False
                )
            ]

        else:
            directories = data_getters.get_dir_content(
                dir_name=str(dir_name), base_name_only=True
            )

    else:
        directories = [dir for dir in os.listdir(data_path / dir_name)]

    directories = [
        dir
        for dir in directories
        if not (dir.startswith(".") or dir.endswith(".txt") or dir.endswith(".zip"))
    ]
    return directories


def load_scotland_data(
    data_path=base_config.ROOT_DATA_PATH,
    rel_data_path=base_config.RAW_SCOTLAND_DATA_PATH,
    batch=None,
    usecols=None,
    n_samples=None,
    load_recs=False,
    dtype=base_config.dtypes,
    low_memory=True,
):
    """Load the Scotland EPC data.

    Args:
        data_path (str/Path, optional): Path to ASF core data directory or 'S3'. Defaults to base_config.ROOT_DATA_PATH.
        rel_data_path (str/Path, optional): Relative path to specific EPC data. Defaults to base_config.RAW_SCOTLAND_DATA_PATH.
        batch (str, optional): Data batch to load. Defaults to None.
        usecols (list, optional): Features/columns to load from EPC dataset. Defaults to None, loading all features.
        n_samples (int, optional): Number of samples/rows to load. Defaults to None, loading all samples.
        load_recs (boolean, optional): Load recommendations instead of certificates (for England/Wales).
        dtype (dict, optional): Dict with dtypes for easier loading.. Defaults to base_config.dtypes.
        low_memory (bool, optional): Whether to load data with low memory. Defaults to True.
            If True, internally process the file in chunks, resulting in lower memory use while parsing,
            but possibly mixed type inference.
            To ensure no mixed types either set False, or specify the type with the dtype parameter.

    Returns:
        pd.DataFrame: Scotland EPC certificate data for given features."""

    if load_recs:
        raise NotImplemented(
            "Loading recommendations is not implemented yet for Scotland. Loading certificates instead."
        )

    RAW_SCOTLAND_DATA_PATH = data_batches.get_batch_path(
        rel_data_path, data_path, batch
    )

    RAW_SCOTLAND_DATA_ZIP = data_batches.get_batch_path(
        base_config.RAW_SCOTLAND_DATA_ZIP,
        data_path,
        batch,
    )

    batch = RAW_SCOTLAND_DATA_PATH.parent.name
    batch_year = str(batch[:4])
    v2_batch = batch_year > 2021
    skiprows = None if v2_batch else 1

    scot_usecols = copy.copy(usecols)

    # If sample file does not exist (probably just not unzipped), unzip the data
    if (str(data_path) != "S3") and not [
        file
        for file in os.listdir(data_path / RAW_SCOTLAND_DATA_PATH)
        if file.endswith(".csv") and file != "Header.csv"
    ]:
        data_download.extract_data(data_path / RAW_SCOTLAND_DATA_ZIP)

    if scot_usecols is not None:

        if "LOCAL_AUTHORITY_LABEL" in scot_usecols:
            scot_usecols.append("DATA_ZONE")
            scot_usecols = [col for col in scot_usecols if col != "LOCAL_AUTHORITY"]

        if v2_batch:

            for i, col in enumerate(scot_usecols):
                if col in base_config.scotland_field_fix_dict.keys():
                    scot_usecols[i] = base_config.scotland_field_fix_dict[col]

            scot_usecols = [
                col
                for col in scot_usecols
                if col not in ["ENERGY_TARIFF", "LMK_KEY", "BUILDING_REFERENCE_NUMBER"]
            ]

        else:
            # Fix columns ("WALLS" features are labeled differently here)
            scot_usecols = [re.sub("WALLS_", "WALL_", col) for col in scot_usecols]
            scot_usecols = [
                re.sub("POSTTOWN", "POST_TOWN", col) for col in scot_usecols
            ]

            if "ENERGY_TARIFF" in scot_usecols:
                scot_usecols.remove("ENERGY_TARIFF")
                scot_usecols.remove("BUILDING_REFERENCE_NUMBER")

            if "UPRN" in scot_usecols:

                scot_usecols.remove("UPRN")
                # scot_usecols.remove("BUILDING_REFERENCE_NUMBER")
                scot_usecols.append("Property_UPRN")

            if "LMK_KEY" in scot_usecols:
                scot_usecols.remove("LMK_KEY")
                scot_usecols.append("OSG_UPRN")

            scot_usecols = [
                col
                for col in scot_usecols
                if col not in base_config.england_wales_only_features
            ]

    files = get_cert_rec_files(data_path, RAW_SCOTLAND_DATA_PATH, scotland_data=True)
    files = [file for file in files if file.endswith(".csv") and file != "Header.csv"]

    epc_certs = [
        data_getters.load_data(
            data_path,
            RAW_SCOTLAND_DATA_PATH / file,
            dtype=dtype,
            low_memory=low_memory,
            usecols=scot_usecols,
            skiprows=skiprows,  # don't load first row (more ellaborate feature names),
            encoding="latin-1",
        )
        for file in files
    ]

    # Concatenate single dataframes into dataframe
    epc_certs = pd.concat(epc_certs, axis=0)

    epc_certs["COUNTRY"] = "Scotland"

    epc_certs = epc_certs.rename(
        columns={
            "WALL_ENV_EFF": "WALLS_ENV_EFF",
            "WALL_ENERGY_EFF": "WALLS_ENERGY_EFF",
            "WALL_DESCRIPTION": "WALLS_DESCRIPTION",
            "POST_TOWN": "POSTTOWN",
            "HEAT_LOSS_CORRIDOOR": "HEAT_LOSS_CORRIDOR",
            "PROPERTY_UPRN": "UPRN",
        },
        errors="ignore",
    )

    if v2_batch:

        clean_dict = {"mÂ²": "m2", "Â£": "£", "ï»¿": ""}
        for col in epc_certs.columns:
            for enc_issue in clean_dict.keys():
                if enc_issue in col:
                    epc_certs.rename(
                        columns={col: col.replace(enc_issue, clean_dict[enc_issue])},
                        inplace=True,
                    )

        epc_certs = epc_certs.rename(columns=base_config.rev_scotland_field_fix_dict)
        epc_certs = epc_certs.rename(columns={"Property_UPRN": "UPRN"})

    # epc_certs["UPRN"] = epc_certs["Property_UPRN"]
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
    load_recs=False,
    dtype=base_config.dtypes,
    low_memory=True,
):
    """Load the England and/or Wales EPC data.

    Args:
        data_path (str/Path, optional): Path to ASF core data directory or 'S3'. Defaults to base_config.ROOT_DATA_PATH.
        rel_data_path (str/Path, optional): Relative path to specific EPC data. Defaults to base_config.RAW_ENG_WALES_DATA_PATH.
        batch (str, optional): Data batch to load. Defaults to None.
        subset (str, optional): Nation subset: 'GB', 'Wales', 'England'. Defaults to None, loading all nation's data.
        usecols (list, optional): Features/columns to load from EPC dataset. Defaults to None, loading all features.
        n_samples (int, optional): Number of samples/rows to load. Defaults to None, loading all samples.
        load_recs (boolean, optional): Load recommendations instead of certificates (for England/Wales).
        dtype (dict, optional): Dict with dtypes for easier loading.. Defaults to base_config.dtypes.
        low_memory (bool, optional): Whether to load data with low memory. Defaults to True.
            If True, internally process the file in chunks, resulting in lower memory use while parsing,
            but possibly mixed type inference.
            To ensure no mixed types either set False, or specify the type with the dtype parameter.

    Returns:
        pd.DataFrame:  England/Wales EPC certificate data for given features."""

    data_to_load = "recommendations" if load_recs else "certificates"
    dtype = base_config.dtypes_recom if load_recs else dtype

    if subset in [None, "GB", "all"]:

        additional_samples = 0

        # Splitting samples across nations
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
            load_recs=load_recs,
            data_check=True,
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
            load_recs=load_recs,
            data_check=False,
        )

        epc_certs = pd.concat([wales_epc, england_epc], axis=0)

        return

    RAW_ENG_WALES_DATA_PATH = data_batches.get_batch_path(
        rel_data_path, data_path, batch
    )

    RAW_ENG_WALES_DATA_ZIP = data_batches.get_batch_path(
        base_config.RAW_ENG_WALES_DATA_ZIP,
        data_path,
        batch,
    )

    # If sample file does not exist (probably just not unzipped), unzip the data
    if (
        str(data_path) != "S3"
        and not Path(
            data_path
            / RAW_ENG_WALES_DATA_PATH
            / "domestic-W06000015-Cardiff/{}.csv".format(data_to_load)
        ).is_file()
    ):
        data_download.extract_data(data_path / RAW_ENG_WALES_DATA_ZIP)

    directories = get_cert_rec_files(data_path, RAW_ENG_WALES_DATA_PATH)

    directories = [
        dir for dir in directories if dir.startswith(start_with_dict[subset])
    ]

    if usecols is not None:
        usecols = [
            col for col in usecols if col not in base_config.scotland_only_features
        ]

    # Load EPC certificates for given subset
    # Only load columns of interest (if given)

    batch = RAW_ENG_WALES_DATA_PATH.parent.name

    if batch in base_config.v0_batches:
        if usecols is not None and "UPRN" in usecols:
            usecols.remove("UPRN")
            usecols.append("BUILDING_REFERENCE_NUMBER")

    epc_certs = [
        data_getters.load_data(
            data_path,
            RAW_ENG_WALES_DATA_PATH / directory / "{}.csv".format(data_to_load),
            dtype=dtype,
            low_memory=low_memory,
            usecols=usecols,
        )
        for directory in directories
    ]

    # # Concatenate single dataframes into dataframe
    epc_certs = pd.concat(epc_certs, axis=0)
    epc_certs["COUNTRY"] = subset

    if "UPRN" in epc_certs.columns and "BUILDING_REFERENCE_NUMBER" in epc_certs.columns:
        epc_certs["UPRN"].fillna(epc_certs.BUILDING_REFERENCE_NUMBER, inplace=True)

        if batch in base_config.v0_batches:
            epc_certs["UPRN"] = epc_certs["BUILDING_REFERENCE_NUMBER"]

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
    load_recs=False,
    dtype=base_config.dtypes,
    data_check=False,
    low_memory=True,
):
    """Load and return EPC dataset, or specific subset, as pandas dataframe.

    Args:
        data_path (str/Path, optional): Path to ASF core data directory or 'S3'. Defaults to base_config.ROOT_DATA_PATH.
        rel_data_path (str/Path, optional): Relative path to specific EPC data. Defaults to base_config.RAW_DATA_PATH.
        batch (str, optional): Data batch to load. Defaults to None.
        subset (str, optional): Nation subset: 'GB', 'Wales', 'England', 'Scotland'. Defaults to "GB", loading all nation's data.
        usecols (list, optional): Features/columns to load from EPC dataset. Defaults to None, loading all features.
        n_samples (int, optional): Number of samples/rows to load. Defaults to None, loading all samples.
        load_recs (boolean, optional): Load recommendations instead of certificates (for England/Wales).
        dtype (dict, optional): Dict with dtypes for easier loading.. Defaults to base_config.dtypes.
        low_memory (bool, optional): Whether to load data with low memory. Defaults to True.
            If True, internally process the file in chunks, resulting in lower memory use while parsing,
            but possibly mixed type inference.
            To ensure no mixed types either set False, or specify the type with the dtype parameter.
    Returns:
        pd.DataFrame: EPC certificate data for given area and features.
    """

    if rel_data_path != base_config.RAW_DATA_PATH:
        RAW_DATA_PATH = data_batches.get_batch_path(rel_data_path, data_path, batch)
        wales_england_path = RAW_DATA_PATH / "England_Wales/"
        scotland_path = RAW_DATA_PATH / "Scotland/"

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

        data_check = True if subset == "Scotland" else False

        epc_scotland_df = load_scotland_data(
            data_path=data_path,
            rel_data_path=scotland_path,
            usecols=usecols,
            data_check=data_check,
            n_samples=None if n_samples is None else n_samples + additional_samples,
            batch=batch,
        )

        all_epc_df.append(epc_scotland_df)

        if subset == "Scotland":
            return epc_scotland_df

    # Get the Wales/England data
    if subset in ["Wales", "England"]:
        epc_df = load_england_wales_data(
            data_path=data_path,
            rel_data_path=wales_england_path,
            subset=subset,
            usecols=usecols,
            n_samples=n_samples,
            load_recs=load_recs,
            dtype=dtype,
            data_check=data_check,
            low_memory=low_memory,
            batch=batch,
        )
        return epc_df

    # Merge the two datasets for GB
    elif subset == "GB":

        for country in ["Wales", "England"]:

            epc_df = load_england_wales_data(
                data_path=data_path,
                rel_data_path=wales_england_path,
                subset=country,
                usecols=usecols,
                n_samples=n_samples,
                load_recs=load_recs,
                dtype=dtype,
                data_check=False,
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

    Args:
        data_path (str/Path, optional): Path to ASF core data directory or 'S3'. Defaults to base_config.ROOT_DATA_PATH.
        rel_data_path (str/Path, optional): Relative path to specific EPC data. Defaults to base_config.EST_CLEANSED_EPC_DATA_DEDUPL_PATH.
        remove_duplicates (bool, optional): Whether or not to remove duplicate property records. Defaults to True.
        usecols (list, optional): Features/columns to load from EPC dataset. Defaults to None, loading all features.
        n_samples (int, optional): Number of samples/rows to load. Defaults to None, loading all samples.

    Returns:
        pd.DataFrame:  Cleansed EPC datast as dataframe.

    """

    rel_data_path = (
        rel_data_path if remove_duplicates else base_config.EST_CLEANSED_EPC_DATA_PATH
    )

    # If file does not exist (probably just not unzipped), unzip the data
    if str(data_path) != "S3" and not (data_path / rel_data_path).is_file():
        data_download.extract_data(data_path / rel_data_path + ".zip")

    print("Loading cleansed EPC data... This will take a moment.")
    cleansed_epc = data_getters.load_data(
        data_path, rel_data_path, usecols=usecols, n_samples=n_samples
    )

    # Drop first column
    if "Unnamed: 0" in cleansed_epc.columns:
        cleansed_epc = cleansed_epc.drop(columns="Unnamed: 0")

    # Add HP feature
    if "FINAL_HEATING_SYSTEM" in cleansed_epc.columns:
        cleansed_epc["HEAT_PUMP"] = cleansed_epc.FINAL_HEATING_SYSTEM == "Heat pump"

    return cleansed_epc


def load_preprocessed_epc_data(
    data_path=base_config.ROOT_DATA_PATH,
    rel_data_path=base_config.RAW_EPC_DATA_PATH.parent,
    subset="GB",
    batch=None,
    version="preprocessed_dedupl",
    usecols=base_config.EPC_PREPROC_FEAT_SELECTION,
    n_samples=None,
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


    Args:
        data_path (str/Path, optional): Path to ASF core data directory or 'S3'. Defaults to base_config.ROOT_DATA_PATH.
        rel_data_path (str/Path, optional): Relative path to specific EPC data. Defaults to base_config.RAW_EPC_DATA_PATH.parent.
        subset (str, optional): Nation subset: 'GB', 'Wales', 'England', 'Scotland'. Defaults to "GB", loading all nation's data.
        batch (str, optional): Data batch to load. Defaults to None.
        version (str, optional): Data version to use. Defaults to "preprocessed_dedupl".
        usecols (list, optional): Features/columns to load from EPC dataset. Defaults to None, loading all features.
        n_samples (int, optional): Number of samples/rows to load. Defaults to None, loading all samples.
        low_memory (bool, optional): Whether to load data with low memory. Defaults to True.
            If True, internally process the file in chunks, resulting in lower memory use while parsing,
            but possibly mixed type inference.
            To ensure no mixed types either set False, or specify the type with the dtype parameter.
    Returns:
        pd.DataFrame: EPC data in the given version
    """

    version_path_dict = {
        "raw": base_config.RAW_EPC_DATA_PATH.name,
        "preprocessed_dedupl": base_config.PREPROC_EPC_DATA_DEDUPL_PATH.name,
        "preprocessed": base_config.PREPROC_EPC_DATA_PATH.name,
    }

    dtype = base_config.dtypes if version == "raw" else base_config.dtypes_prepr

    EPC_DATA_PATH = data_batches.get_batch_path(
        rel_data_path / version_path_dict[version], data_path, batch
    )

    # If file does not exist (likely just not unzipped), unzip the data
    if (str(data_path) != "S3") and not (data_path / EPC_DATA_PATH).is_file():
        data_download.extract_data(
            data_path / EPC_DATA_PATH.parent / (EPC_DATA_PATH.name + ".zip")
        )

    epc_df = data_getters.load_data(
        data_path,
        EPC_DATA_PATH,
        dtype=dtype,
        low_memory=low_memory,
        usecols=usecols,
        n_samples=n_samples,
    )

    for col in base_config.parse_dates:
        if col in epc_df.columns:
            epc_df[col] = pd.to_datetime(epc_df[col], errors="coerce")

    if subset is not None and subset != "GB":
        epc_df = epc_df.loc[epc_df["COUNTRY"] == subset]

    return epc_df


def get_epc_sample(full_df, sample_size):
    """Randomly sample a subset of the full data.

    Args:
        full_df (pandas.DataFrame): Full dataframe from which to extract a subset.
        sample_size (int): Number of samples (size of subset).

    Returns:
        pandas.DataFrame: Randomly sampled subset of full dataframe
    """

    rand_ints = np.random.choice(len(full_df), size=sample_size)
    sample_df = full_df.iloc[rand_ints]

    return sample_df


def filter_by_year(
    epc_df, year, building_identifier="UPRN", up_to=True, selection=None
):
    """Filter EPC dataset by year of inspection/entry.

    Args:
        epc_df (pandas.DataFrame): Dataframe to which new features are added.
        year (int): Year by which to filter data.
        building_identifier (str): Building identifier, e.g. UPRN or BUILDING_REFERENCE_NUMBER. Defaults to "UPRN".
        up_to (bool, optional):  If True, get all samples up to given year.
            If False, only get sample from given year. Defaults to True.
            If False, only given year data will be loaded.
        selection (str, optional): For duplicates, get only "first entry" or "latest entry".
            If None, do not remove any duplicates. Defaults to None.

    Returns:
        pandas.DataFrame: Reduced data with only years of interest.
    """

    # If year is given for filtering
    if year != "all" and year is not None:

        if up_to:
            epc_df = epc_df.loc[epc_df["INSPECTION_DATE"].dt.year <= year]
        else:
            epc_df = epc_df.loc[epc_df["INSPECTION_DATE"].dt.year == year]

    # Filter by selection
    selection_dict = {"first entry": "first", "latest entry": "last"}

    if selection in ["first entry", "latest entry"]:

        epc_df = (
            epc_df.sort_values("INSPECTION_DATE", ascending=True)
            .drop_duplicates(
                subset=[building_identifier], keep=selection_dict[selection]
            )
            .sort_index()
        )

    elif selection is None:
        epc_df = epc_df

    else:
        raise IOError("{} not implemented.".format(selection))

    return epc_df


def main():
    """Main function for testing."""

    epc = load_preprocessed_epc_data(version="preprocessed_dedupl")


if __name__ == "__main__":
    # Execute only if run as a script
    main()
