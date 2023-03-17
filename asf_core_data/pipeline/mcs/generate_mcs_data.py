# asf_core_data/pipeline/mcs/generate_mcs_data.py
# Functions for generating processed versions of MCS data (both installations and installers) and saving to S3.

from datetime import date
import pandas as pd
import warnings
import re
import datetime
import os

from asf_core_data.config import base_config

from asf_core_data.getters.data_getters import (
    s3,
    load_s3_data,
    save_to_s3,
    get_s3_dir_files,
    get_most_recent_batch_name,
)

from asf_core_data.pipeline.mcs.process.process_mcs_installations import (
    get_processed_installations_data,
)

from asf_core_data.pipeline.mcs.process.mcs_epc_joining import (
    join_mcs_epc_data,
    select_most_relevant_epc,
)

from asf_core_data.pipeline.mcs.process.process_mcs_utils import (
    colnames_dict,
)

from asf_core_data.pipeline.mcs.process.process_historical_mcs_installers import (
    preprocess_historical_installers,
)
from asf_core_data.getters.mcs_getters.get_mcs_installers import (
    get_most_recent_raw_historical_installers_data,
)
from asf_core_data.getters.mcs_getters.get_mcs_installations import (
    get_most_recent_raw_historical_installations_data,
)

bucket_name = base_config.BUCKET_NAME
mcs_installations_path = base_config.MCS_INSTALLATIONS_PATH
mcs_installations_epc_full_path = base_config.MCS_INSTALLATIONS_EPC_FULL_PATH

mcs_installations_epc_newest_path = base_config.MCS_INSTALLATIONS_EPC_NEWEST_PATH
mcs_installations_epc_most_relevant_path = (
    base_config.MCS_INSTALLATIONS_EPC_MOST_RELEVANT_PATH
)

installations_raw_s3_path = base_config.INSTALLATIONS_RAW_S3_PATH
installers_raw_s3_path = base_config.MCS_RAW_INSTALLER_CONCAT_S3_PATH
raw_data_s3_folder = base_config.RAW_DATA_S3_FOLDER
keyword_to_path_dict = {
    "none": mcs_installations_path,
    "full": mcs_installations_epc_full_path,
    "newest": mcs_installations_epc_newest_path,
    "most_relevant": mcs_installations_epc_most_relevant_path,
}


def generate_and_save_mcs(
    uk_geo_data: pd.DataFrame,
    epc_data_path: str = base_config.ROOT_DATA_PATH,
    companies_house_api_key: str = os.environ.get("COMPANIES_HOUSE_API_KEY"),
    verbose=False,
):
    """Concatenates, generates and saves the different versions of the MCS-EPC data to S3.
    Different versions are a) just installation data, b) installation data with
    each property's entire EPC history attached, c) with the EPC corresponding
    to the most recent inspection and d) with the most recent EPC from before
    the HP installation if one exists or the earliest EPC from after the HP
    installation otherwise.
    """
    today = date.today().strftime("%y%m%d")

    no_epc_path, full_epc_path, _, most_relevant_epc_path = [
        path_stem.format(today)
        for path_stem in [
            mcs_installations_path,
            mcs_installations_epc_full_path,
            mcs_installations_epc_newest_path,
            mcs_installations_epc_most_relevant_path,
        ]
    ]

    # legacy function calls in the 3 lines below
    # all_installations_data, all_installer_data = get_latest_mcs_from_s3()
    # if verbose:
    #         print("Installations files")
    #         print("\n".join([file[0] for file in all_installations_data]))
    #         print("\nInstaller files")
    #         print("\n".join([file[0] for file in all_installer_data]))

    # concatenate_save_raw_installations(all_installations_data)
    # concatenate_save_raw_installers(all_installer_data)

    all_installations_data = get_most_recent_raw_historical_installations_data()
    all_installers_data = get_most_recent_raw_historical_installers_data()

    # process historical installers (needs to happen before processing historical installations)
    processed_historical_installers = preprocess_historical_installers(
        raw_historical_installers=all_installers_data,
        raw_historical_installations=all_installations_data,
        geographical_data=uk_geo_data,
        companies_house_api_key=companies_house_api_key,
    )

    # save historical installers
    date_historical_installers_received = get_most_recent_batch_name(
        bucket=bucket_name,
        s3_folder_path=base_config.MCS_HISTORICAL_DATA_INPUTS_PATH,
        filter_keep_keywords=["installer"],
    )
    date_historical_installers_received = date_historical_installers_received.split(
        "installers_"
    )[1].split(".xlsx")[0]

    installers_path = (
        base_config.PREPROCESSED_MCS_HISTORICAL_INSTALLERS_FILE_PATH.format(
            date_historical_installers_received
        )
    )
    save_to_s3(
        s3=s3,
        bucket_name=bucket_name,
        output_var=processed_historical_installers,
        output_file_path=installers_path,
    )
    print("Saved in S3: " + installers_path)

    processed_mcs = get_processed_installations_data()
    save_to_s3(s3, bucket_name, processed_mcs, no_epc_path)
    print("Saved in S3: " + no_epc_path)

    fully_joined_mcs_epc = join_mcs_epc_data(
        epc_data_path=epc_data_path,
        hps=processed_mcs,
        all_records=True,
        verbose=verbose,
    )
    save_to_s3(s3, bucket_name, fully_joined_mcs_epc, full_epc_path)
    print("Saved in S3: " + full_epc_path)

    # avoid completely regenerating the joined df by just filtering it
    # make sure INSPECTION_DATE column is a date
    # fully_joined_mcs_epc["INSPECTION_DATE"] = pd.to_datetime(fully_joined_mcs_epc["INSPECTION_DATE"])
    # newest_mcs_epc = fully_joined_mcs_epc.loc[
    #     fully_joined_mcs_epc.groupby("original_mcs_index")["INSPECTION_DATE"].idxmax()
    # ]
    # save_to_s3(s3, bucket_name, newest_mcs_epc, newest_epc_path)
    # print("Saved in S3: " + newest_epc_path)

    most_relevant_mcs_epc = select_most_relevant_epc(fully_joined_mcs_epc)
    save_to_s3(s3, bucket_name, most_relevant_mcs_epc, most_relevant_epc_path)
    print("Saved in S3: " + most_relevant_epc_path)


### ---- Legacy functions ----
# The functions and variables below are legacy functions/variables that are no longer in use.
installations_raw_s3_path = base_config.INSTALLATIONS_RAW_S3_PATH
installers_raw_s3_path = base_config.MCS_RAW_INSTALLER_CONCAT_S3_PATH
raw_data_s3_folder = base_config.RAW_DATA_S3_FOLDER


def get_latest_mcs_from_s3():
    """
    --- Legacy function ---
    Gets latest installations and installer MCS data from latest_raw_data s3 path.
    """

    mcs_files = [
        key
        for key in get_s3_dir_files(path_to_dir=raw_data_s3_folder)
        if ("installations" or "installer" in key) and ("historic" not in key)
    ]

    installer_data = []
    installations_data = []

    for file in mcs_files:
        if "installations" in file:

            installations = load_s3_data(bucket_name, file)
            if type(installations) == pd.DataFrame:
                installations_data.append((file, installations))
            elif type(installations) == dict:
                installations_key = " ".join(
                    [
                        key
                        for key in list(installations.keys())
                        if "installs" in key.lower()
                    ]
                )
                installer_key = " ".join(
                    [
                        key
                        for key in list(installations.keys())
                        if "installer" in key.lower()
                    ]
                )

                installer_data.append((file, installations[installer_key]))
                installations_data.append((file, installations[installations_key]))
        elif "installer" in file:
            installers = load_s3_data(bucket_name, file)
            installer_data.append((file, installers))

    return installations_data, installer_data


def concatenate_save_raw_installations(all_installations_data):
    """
    --- Legacy function ---
    Generate concatenated installation csv from individual installation files.
    concatenates and saves the result in the top level MCS inputs folder.
    While doing so, checks whether any records in the files are outside the
    year and quarter stated in the filename, and flags if file columns differ.
    """
    for key_and_df in all_installations_data:

        year_quarter_search = re.search(r"20[0-9][0-9]_q[1-4]", key_and_df[0])
        if year_quarter_search:  # ignore mcs_installations_2021.xlsx
            year_quarter = year_quarter_search[0]  # get match
            year, quarter = int(year_quarter[0:4]), int(year_quarter[-1])
            end_month = quarter * 3
            start_date = datetime.date(year, end_month - 2, 1)

            if quarter == 4:
                end_date = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
            else:
                end_date = datetime.date(year, end_month + 1, 1) - datetime.timedelta(
                    days=1
                )

            if (
                (
                    pd.to_datetime(key_and_df[1]["Commissioning Date"]).dt.date
                    < start_date
                )
                | (
                    pd.to_datetime(key_and_df[1]["Commissioning Date"]).dt.date
                    > end_date
                )
            ).any():
                warnings.warn(
                    "{} has some records outside its stated quarter.".format(
                        key_and_df[0]
                    )
                )

    installations_dfs = [key_and_df[1] for key_and_df in all_installations_data]

    if len(set([tuple(df.columns) for df in installations_dfs])) > 1:
        warnings.warn("Not all installation file columns are the same.")

    concat_installations = pd.concat(installations_dfs)
    print(
        "Number of records before removing duplicates:", concat_installations.shape[0]
    )
    concat_installations.drop_duplicates(inplace=True, ignore_index=True)
    print("Number of records after removing duplicates:", concat_installations.shape[0])

    concat_installations = concat_installations.rename(columns=colnames_dict)

    save_to_s3(bucket_name, concat_installations, "/" + installations_raw_s3_path)


def concatenate_save_raw_installers(all_installer_data):
    """
    --- Legacy function ---
    Generate concatenated installer csv from individual installer files.
    concatenates and saves the result in the top level MCS inputs folder
    """
    installer_dfs = [key_and_df[1] for key_and_df in all_installer_data]
    concat_installers = pd.concat(installer_dfs)
    concat_installers.drop_duplicates(inplace=True, ignore_index=True)

    save_to_s3(bucket_name, concat_installers, "/" + installers_raw_s3_path)


def generate_processed_mcs_installations(
    epc_version="none", epc_data_path=base_config.ROOT_DATA_PATH
):
    """
    --- Legacy function ---
    Generates processed version of MCS installation data (with optional
    joined EPC data) from the raw data.
    Args:
        epc_version (str, optional): One of "none", "full", "newest" or "most_relevant".
        "none" returns just installation data, "full" returns installation data with
        each property's entire EPC history attached, "newest" selects the EPC
        corresponding to the most recent inspection and "most_relevant" selects the
        most recent EPC from before the HP installation if one exists or the earliest EPC
        from after the HP installation otherwise. Defaults to "none".
    Raises:
        ValueError: if epc_version is not one of the specified values.
    Returns:
        DataFrame: installation (+ EPC) data.
    """
    if epc_version not in ["none", "full", "newest", "most_relevant"]:
        raise ValueError(
            "epc_version should be one of 'none', 'full', 'newest' or 'most_relevant'"
        )

    processed_mcs = get_processed_installations_data()

    if epc_version == "none":
        return processed_mcs
    else:
        joined_mcs_epc = join_mcs_epc_data(
            hps=processed_mcs, epc_data_path=epc_data_path
        )
        if epc_version == "newest":
            processed_mcs = joined_mcs_epc.loc[
                joined_mcs_epc.groupby("original_mcs_index")["INSPECTION_DATE"].idxmax()
            ]
        elif epc_version == "most_relevant":
            processed_mcs = select_most_relevant_epc(joined_mcs_epc)
        else:
            processed_mcs = joined_mcs_epc

    return processed_mcs


def get_mcs_installations(epc_version="none", refresh=False):
    """
    --- Legacy function ---
    Gets MCS (+ EPC) data. Tries to get the most recent version
    from S3 if one exists.

    Args:
        epc_version (str, optional): One of "none", "full", "newest" or "most_relevant".
            "none" returns just installation data, "full" returns installation data with
            each property's entire EPC history attached, "newest" selects the EPC
            corresponding to the most recent inspection and "most_relevant" selects the
            most recent EPC from before the HP installation if one exists or the earliest EPC
            from after the HP installation otherwise. Defaults to "none".
        refresh (bool, optional): If True, skips the S3 check and generates the processed
            data from raw data. Defaults to False.
    Returns:
        DataFrame: installation (+ EPC) data.
    """
    if not refresh:
        bucket = s3.Bucket(bucket_name)
        folder = "outputs/MCS/"
        file_list = [
            ("/" + object.key) for object in bucket.objects.filter(Prefix=folder)
        ]  # bit of a hack
        file_prefix = keyword_to_path_dict[epc_version].split("{")[0]
        matches = [
            filename
            for filename in file_list
            if (re.split("[0-9]", filename)[0] == file_prefix)
        ]
        if len(matches) > 0:
            latest_version = max(matches)
            print("Loading", latest_version, "from S3")
            mcs_installations = load_s3_data(
                bucket_name, latest_version[1:]  # undoing the hack
            )
            return mcs_installations
        else:
            print("File not found on S3.")

    return mcs_installations


if __name__ == "__main__":
    uk_geo_path = base_config.POSTCODE_TO_COORD_PATH
    uk_geo_data = load_s3_data(bucket_name, uk_geo_path)

    generate_and_save_mcs(uk_geo_data=uk_geo_data)
