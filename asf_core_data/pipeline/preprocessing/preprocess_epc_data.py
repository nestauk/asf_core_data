# File: asf_core_data/pipeline/preprocessing/preprocess_epc_data.py
"""Loading and preprocessing the raw EPC data for England, Wales and Scotland."""

# ----------------------------------------------------------------------------------

import re
import time
import os
import logging


from asf_core_data.pipeline.preprocessing import data_cleaning, feature_engineering
from asf_core_data.getters.epc import epc_data, data_batches
from asf_core_data.config import base_config
from asf_core_data import Path
from argparse import ArgumentParser

# ----------------------------------------------------------------------------------


def preprocess_data(
    df,
    remove_duplicates=True,
    data_path=base_config.ROOT_DATA_PATH,
    subset="GB",
    batch=None,
    save_data=base_config.PREPROC_EPC_DATA_PATH,
    verbose=True,
):
    """Preprocess the raw EPC data by cleaning it and removing duplications.
    The data at the different processing steps can be saved.

    The processing steps:

    - raw:
    Concatenated EPC records for all countries but otherwise not processed EPC data

    - preprocessed:
    Partially cleaned and with additional features

    - preprocessed_dedupl:
    Same as 'preprocessed' but without duplicates

    Args:
        df (pandas.DataFrame): Dataframe holding EPC data to process.
        remove_duplicates (bool, optional): Whether or not to remove duplicates.. Defaults to True.
        data_path (str/Path, optional): Path to ASF core data directory or 'S3'. Defaults to base_config.ROOT_DATA_PATH.
        subset (str, optional): Nation subset: "England", "Wales" or "Scotland", will adjust outfile path.
        batch (str, optional): Data batch to load. Defaults to None.
        save_data (str/Path):  Where to preprocessed data at different stages (original, cleaned, deduplicated).
            None does not save the outputs. Defaults to base_config.PREPROC_EPC_DATA_PATH.
        verbose (bool, optional): Print number of features and samples after each processing step. Defaults to True.

    Returns:
        pandas.DataFrame: Preprocessed EPC dataset.
    """

    # --------------------------------
    # Raw data
    # --------------------------------

    if save_data is not None:
        file_path = data_batches.get_batch_path(
            data_path / base_config.RAW_EPC_DATA_PATH,
            data_path=data_path,
            batch=batch,
            check_folder="inputs",
        )

        if subset != "GB":
            file_path = re.sub(
                "GB",
                subset,
                str(file_path),
            )
        print("Saving raw data to {}".format(file_path))
        print()

        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        # Save unaltered_version
        df.to_csv(file_path, index=False)

    processing_steps = []
    processing_steps.append(("Original data", df.shape[0], df.shape[1]))

    # --------------------------------
    # Preprocessing data
    # --------------------------------

    df = data_cleaning.clean_epc_data(df)
    processing_steps.append(("After cleaning", df.shape[0], df.shape[1]))

    df = feature_engineering.get_additional_features(df)
    processing_steps.append(("After adding features", df.shape[0], df.shape[1]))

    if save_data is not None:
        file_path = data_batches.get_batch_path(
            data_path / base_config.PREPROC_EPC_DATA_PATH,
            data_path=data_path,
            batch=batch,
            check_folder="outputs",
        )

        if subset != "GB":
            file_path = re.sub(
                "GB",
                subset,
                str(file_path),
            )

        print("Saving preprocessed data to {}".format(file_path))
        print()

        # Save unaltered_version
        df.to_csv(file_path, index=False)

    # --------------------------------
    # Deduplicated data
    # --------------------------------

    if remove_duplicates:
        df = epc_data.filter_by_year(
            df, None, building_identifier="UPRN", selection="latest entry"
        )

        processing_steps.append(("After removing duplicates", df.shape[0], df.shape[1]))

        if save_data is not None:
            file_path = data_batches.get_batch_path(
                data_path / base_config.PREPROC_EPC_DATA_DEDUPL_PATH,
                data_path=data_path,
                batch=batch,
                check_folder="outputs",
            )

            if subset != "GB":
                file_path = re.sub(
                    "GB",
                    subset,
                    str(file_path),
                )

            print("Saving preprocessed and deduplicated data to {}".format(file_path))
            print()

            # Save unaltered_version
            df.to_csv(file_path, index=False)

    # --------------------------------
    # Print stats
    # --------------------------------

    if verbose:
        for step in processing_steps:
            print("{}:\t{} samples, {} features".format(step[0], step[1], step[2]))

    return df


def load_and_preprocess_epc_data(
    data_path=base_config.ROOT_DATA_PATH,
    rel_data_path=base_config.RAW_DATA_PATH,
    subset="GB",
    usecols=base_config.EPC_FEAT_SELECTION,
    batch=None,
    n_samples=None,
    remove_duplicates=True,
    save_data=base_config.PREPROC_EPC_DATA_PATH,
    reload_raw=False,
):
    """Load and preprocess the EPC data.

    Args:
        subset (str, optional): Nation subset: "England", "Wales" or "Scotland". Defaults to "GB", loading both England and Wales data.
        data_path (str/Path, optional): Path to ASF core data directory or 'S3'. Defaults to base_config.ROOT_DATA_PATH.
        rel_data_path (str/Path, optional): Relative path to specific EPC data. Defaults to base_config.EPC_FEAT_SELECTION.
        subset (str, optional): Nation subset: "England", "Wales" or "Scotland". Defaults to "GB", loading all data.
        usecols (list, optional): List of features/columns to load from EPC dataset.
            By default, a pre-selected list of features (specified in the config file) is used.
            If None, then all features will be loaded. Defaults to None.
        batch (str, optional): Data batch to load. Defaults to None.
        n_samples (int, optional): Number of samples/rows to load. Defaults to None, loading all samples.
        remove_duplicates (bool, optional): Whether or not to remove duplicates for same property. Defaults to True.
        save_data (str/Path, optional): Whether or not to save preprocessed data at different stages (original, cleaned, deduplicated).
            Defaults to base_config.PREPROC_EPC_DATA_PATH.
        reload_raw (bool, optional): Whether to reload the individual raw EPC records from inputs folder
            or whether to use  the fully concatenated raw EPC data from the outputs folder (still unprocessed).
            Reloading can be useful if there have been changes to the input data or the loading functions. Defaults to False.

    Returns:
        pandas.DataFrame:  Preprocessed EPC dataset.
    """

    # Default to False
    raw_epc_exists = False

    # Do not save/overwrite the preprocessed data when not loading entire GB dataset
    # in order to prevent confusion.
    if str(save_data) == "S3":
        save_data = None
    else:
        raw_data_path = data_batches.get_batch_path(
            base_config.RAW_EPC_DATA_PATH, data_path, batch, check_folder="inputs"
        )

        # Raw EPC can be found in data path?
        raw_epc_exists = (Path(data_path) / raw_data_path).is_file()

    if n_samples is not None:
        save_data = None
        logging.warning(
            "You're not loading all samples so the processed data will not be saved!"
        )

    if save_data is not None and not os.path.isabs(save_data):
        save_data = data_path / save_data

        if subset != "GB":
            logging.warning("Careful! You're not loading the complete GB dataset.")

    if subset != "GB" and n_samples is not None:
        logging.warning(
            "Nation subsets do not work well in combination with low n_samples. Set n_samples=None for best results."
        )

    # Load raw EPC data (for all of GB or subset)
    # ++++++++++++++++++++++++++++++++++++++++++++
    # There's two options, but if all data sources are up-to-date, the output is the same.
    # Option a) is faster and more general, while Option b) is useful when testing
    # loading functions and after updating input data of existing batch.

    # a) Load the concatenated CSV file from outputs (EPC_GB_raw.csv).
    # If available, this is slightly faster as only one file needs to be loaded.
    # This concatenated version is generated when processing the EPC data
    # with preprocess_data() or load_and_preprocess_epc_data().
    # This is also the only option when loading the data directly from S3.

    # b) Load the EPC data from the individual record files for England, Wales and Scotland in the inputs folder.
    # This may take slightly longer to load as several files need to be read and concatenated.
    # However, this option is useful when testing updated EPC loading functions or when updating
    # an existing EPC data batch.
    # This option is not (yet) available when loading data directly from S3.

    # Option a): Load raw EPC data (concatenated but not actually processed yet)
    if (raw_epc_exists and not reload_raw) or str(data_path) == "S3":
        # Load raw EPC from outputs folder for given batch (England/Wales/Scotland combined in EPC_GB_raw.csv)
        epc_df = epc_data.load_preprocessed_epc_data(
            data_path=data_path,
            subset=subset,
            batch=batch,
            version="raw",
            usecols=usecols + ["COUNTRY"],
            n_samples=n_samples,
        )

    # Option b): Load the raw EPC data for given batch from individual EPC record files
    # in inputs folder and concatenate the outputs for all countries.
    # The feature 'COUNTRY' will be added as the data is loaded.
    else:
        epc_df = epc_data.load_raw_epc_data(
            data_path=data_path,
            rel_data_path=rel_data_path,
            subset=subset,
            batch=batch,
            usecols=usecols,
            n_samples=n_samples,
        )

    # Process the EPC data
    # ++++++++++++++++++++++++++++++++++++++++++++
    # After loading the raw EPC data with any of the two options above,
    # put it through the processing pipeline.
    # This will create a raw, processed and preocessed and deduplicated version,
    # meaning that EPC_GB_raw.csv will be created/updated so it can be used next time for direct loading.

    # Now process the loaded EPC data
    epc_df = preprocess_data(
        epc_df,
        data_path=data_path,
        subset=subset,
        remove_duplicates=remove_duplicates,
        save_data=save_data,
        batch=batch,
    )
    return epc_df


# ---------------------------------------------------------------------------------
def create_argparser() -> ArgumentParser:
    """
    Creates an argument parser that can receive the following arguments:
    - path_to_data: either local path to where data is stored or "S3"
    """
    parser = ArgumentParser()

    parser.add_argument(
        "--path_to_data",
        help="Path to data",
        default="S3",
        type=str,
    )

    return parser


if __name__ == "__main__":
    parser = create_argparser()
    args = parser.parse_args()

    LOCAL_DATA_DIR = args.path_to_data

    start_time = time.time()

    print("Loading and preprocessing EPC data... This will take a while.\n")
    load_and_preprocess_epc_data(data_path=LOCAL_DATA_DIR)

    end_time = time.time()
    runtime = round((end_time - start_time) / 60)

    print("\nLoading and preprocessing the EPC data took {} minutes.".format(runtime))
