# File: asf_core_data/pipeline/preprocessing/preprocess_epc_data.py
"""Loading and preprocessing the raw EPC data for England, Wales and Scotland."""

# ----------------------------------------------------------------------------------

from imp import reload
import re
import time
import os
import logging


from asf_core_data.pipeline.preprocessing import data_cleaning, feature_engineering
from asf_core_data.getters.epc import epc_data, data_batches
from asf_core_data.config import base_config
from asf_core_data import Path

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
    Merged but otherwise not altered EPC data

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
        reload_raw (bool, optional): Whether to reload the raw EPC data from inputs folder or whether to use the fully
            merged raw EPC data from the outputs folder (still unprocessed). Reloading can be useful if there have been changes to the raw data or the loading functions. Defaults to False.

    Returns:
        pandas.DataFrame:  Preprocessed EPC dataset.
    """

    # Do not save/overwrite the preprocessed data when not loading entire GB dataset
    # in order to prevent confusion.

    # Default to False
    raw_epc_exists = False

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

    # Load raw data (from 'preprocessed' and combined raw data or 'untouched' raw data)
    # Shouldn't make a difference but sometimes useful when debugging or after updating data
    if (raw_epc_exists and not reload_raw) or str(data_path) == "S3":

        # Load raw EPC from preprocessed batch in outputs foler (England/Wales/Scotland combined in EPC_GB_raw.csv)
        epc_df = epc_data.load_preprocessed_epc_data(
            data_path=data_path,
            subset=subset,
            batch=batch,
            version="raw",
            usecols=usecols + ["COUNTRY"],
            n_samples=n_samples,
        )

    else:
        # Load raw EPC data from England/Wales and Scotland data from raw data folders (inputs)
        epc_df = epc_data.load_raw_epc_data(
            data_path=data_path,
            rel_data_path=rel_data_path,
            subset=subset,
            batch=batch,
            usecols=usecols,
            n_samples=n_samples,
        )

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


def main():
    """Main function: Loads and preprocessed EPC data with default settings."""

    ASF_CORE_DATA_DIR = "/Users/juliasuter/Documents/ASF_data"

    start_time = time.time()

    print("Loading and preprocessing EPC data... This will take a while.\n")
    epc_df = load_and_preprocess_epc_data(
        usecols=base_config.EPC_FEAT_SELECTION,
        n_samples=None,
        save_data=base_config.PREPROC_EPC_DATA_PATH,
        data_path=ASF_CORE_DATA_DIR,
    )

    end_time = time.time()
    runtime = round((end_time - start_time) / 60)

    print("\nLoading and preprocessing the EPC data took {} minutes.".format(runtime))


if __name__ == "__main__":
    # Execute only if run as a script
    main()
