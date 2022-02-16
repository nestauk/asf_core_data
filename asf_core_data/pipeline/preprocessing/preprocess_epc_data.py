# File: heat_pump_adoption_modelling/pipeline/preprocessing/preprocess_epc_data.py
"""Loading and preprocessing the raw EPC data for England, Wales and Scotland."""

# ----------------------------------------------------------------------------------

import time

from asf_core_data import PROJECT_DIR, get_yaml_config, Path

from asf_core_data.pipeline.preprocessing import (
    feature_engineering,
    data_cleaning,
)
from asf_core_data.getters.epc import epc_data

# ----------------------------------------------------------------------------------

# Load config file
config = get_yaml_config(Path(str(PROJECT_DIR) + "/asf_core_data/config/base.yaml"))

# Get paths
RAW_EPC_DATA_PATH = str(PROJECT_DIR) + config["RAW_EPC_DATA_PATH"]
PREPROC_EPC_DATA_PATH = str(PROJECT_DIR) + config["PREPROC_EPC_DATA_PATH"]
PREPROC_EPC_DATA_DEDUPL_PATH = str(PROJECT_DIR) + config["PREPROC_EPC_DATA_DEDUPL_PATH"]

EPC_FEAT_SELECTION = config["EPC_FEAT_SELECTION"]


def preprocess_data(df, remove_duplicates=True, save_data=True, verbose=True):
    """Preprocess the raw EPC data by cleaning it and removing duplications.
    The data at the different processing steps can be saved.

    The processing steps:

    - raw:
    Merged but otherwise not altered EPC data

    - preprocessed:
    Partially cleaned and with additional features

    - preprocessed_dedupl:
    Same as 'preprocessed' but without duplicates

    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe holding EPC data to process.

    remove_duplicates : bool, default=True
        Whether or not to remove duplicates.

    save_data : bool, default=True
        Whether or not to save preprocessed data at different stages (original, cleaned, deduplicated).

    verbose : bool, default=True
        Print number of features and samples after each processing step.

    Return
    ---------
    df : pandas.DataFrame
        Preprocessed EPC dataset."""

    # --------------------------------
    # Raw data
    # --------------------------------

    if save_data:
        # Save unaltered_version
        df.to_csv(RAW_EPC_DATA_PATH, index=False)

    processing_steps = []
    processing_steps.append(("Original data", df.shape[0], df.shape[1]))

    # --------------------------------
    # Preprocessing data
    # --------------------------------

    df = data_cleaning.clean_epc_data(df)
    processing_steps.append(("After cleaning", df.shape[0], df.shape[1]))

    df = feature_engineering.get_additional_features(df)
    processing_steps.append(("After adding features", df.shape[0], df.shape[1]))

    if save_data:
        # Save unaltered_version
        df.to_csv(PREPROC_EPC_DATA_PATH, index=False)

    # --------------------------------
    # Deduplicated data
    # --------------------------------

    if remove_duplicates:

        df = feature_engineering.filter_by_year(
            df, "BUILDING_ID", None, selection="latest entry"
        )

        processing_steps.append(("After removing duplicates", df.shape[0], df.shape[1]))

        if save_data:
            # Save unaltered_version
            df.to_csv(PREPROC_EPC_DATA_DEDUPL_PATH, index=False)

    # --------------------------------
    # Print stats
    # --------------------------------

    if verbose:

        for step in processing_steps:
            print("{}:\t{} samples, {} features".format(step[0], step[1], step[2]))

    return df


def load_and_preprocess_epc_data(
    subset="GB",
    usecols=EPC_FEAT_SELECTION,
    nrows=None,
    remove_duplicates=True,
    save_data=True,
):
    """Load and preprocess the EPC data.

    Parameters
    ----------
    subset : {'GB', 'Wales', 'England', 'Scotland', None}, default='GB'
        EPC certificate area subset.

    usecols : list, default=EPC_FEAT_SELECTION
        List of features/columns to load from EPC dataset.
        By default, a pre-selected list of features (specified in the config file) is used.
        If None, then all features will be loaded.

    nrows : int, default=None
        Number of rows of file to read.

    remove_duplicates : bool, default=True
        Whether or not to remove duplicates.

    save_data : bool, default=True
        Whether or not to save preprocessed data at different stages (original, cleaned, deduplicated).


    Return
    ---------
    epc_df : pandas.DataFrame
        Preprocessed EPC dataset."""

    # Do not save/overwrite the preprocessed data when not loading entire GB dataset
    # in order to prevent confusion.
    if subset != "GB":
        print(
            "The preprocessed data will be returned but not be written to file. Change subset to 'GB' or save processed data manually."
        )
        save_data = False

    epc_df = epc_data.load_raw_epc_data(subset=subset, usecols=usecols, nrows=nrows)
    epc_df = preprocess_data(
        epc_df, remove_duplicates=remove_duplicates, save_data=save_data
    )
    return epc_df


# ---------------------------------------------------------------------------------


def main():
    """Main function: Loads and preprocessed EPC data with default settings."""

    start_time = time.time()

    print("Loading and preprocessing EPC data... This will take a while.\n")
    epc_df = load_and_preprocess_epc_data(
        usecols=EPC_FEAT_SELECTION
        + [
            "SOLAR_WATER_HEATING_FLAG",
            "FLOOR_HEIGHT",
            "WIND_TURBINE_COUNT",
            "PHOTO_SUPPLY",
            "FLOOR_LEVEL",
            "NUMBER_HEATED_ROOMS",
            "MECHANICAL_VENTILATION",
            "MAIN_HEATING_CONTROLS",
            "MULTI_GLAZE_PROPORTION",
            "GLAZED_TYPE",
            "GLAZED_AREA",
            "EXTENSION_COUNT",
            "SECONDHEAT_DESCRIPTION",
            "ADDRESS2",
        ],
        nrows=None,
    )

    end_time = time.time()
    runtime = round((end_time - start_time) / 60)

    print("\nLoading and preprocessing the EPC data took {} minutes.".format(runtime))

    print(epc_df.shape)
    print(epc_df.head())


if __name__ == "__main__":
    # Execute only if run as a script
    main()
