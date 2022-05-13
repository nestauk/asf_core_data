# File: heat_pump_adoption_modelling/pipeline/preprocessing/preprocess_epc_data.py
"""Loading and preprocessing the raw EPC data for England, Wales and Scotland."""

# ----------------------------------------------------------------------------------

import re
import time
import os


import asf_core_data
from asf_core_data.pipeline.preprocessing import (
    feature_engineering,
    data_cleaning,
)

from asf_core_data.getters.epc import epc_data
from asf_core_data.config import base_config

import warnings

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

    if save_data is not None:
        file_path = epc_data.get_version_path(
            data_path / base_config.RAW_EPC_DATA_PATH,
            data_path=data_path,
            batch=batch,
        )

        if subset != "GB":
            file_path = re.sub(
                "GB",
                subset,
                str(file_path),
            )
        print("Saving to raw data to {}".format(file_path))
        print()
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

        file_path = epc_data.get_version_path(
            data_path / base_config.PREPROC_EPC_DATA_PATH,
            data_path=data_path,
            batch=batch,
        )

        if subset != "GB":
            file_path = re.sub(
                "GB",
                subset,
                str(file_path),
            )

        print("Saving to preprocessed data to {}".format(file_path))
        print()
        # Save unaltered_version
        df.to_csv(file_path, index=False)

    # --------------------------------
    # Deduplicated data
    # --------------------------------

    if remove_duplicates:

        df = feature_engineering.filter_by_year(
            df, "UPRN", None, selection="latest entry"
        )

        processing_steps.append(("After removing duplicates", df.shape[0], df.shape[1]))

        if save_data is not None:

            file_path = epc_data.get_version_path(
                data_path / base_config.PREPROC_EPC_DATA_DEDUPL_PATH,
                data_path=data_path,
                batch=batch,
            )

            if subset != "GB":
                file_path = re.sub(
                    "GB",
                    subset,
                    str(file_path),
                )

            print(
                "Saving to preprocessed and deduplicated data to {}".format(file_path)
            )
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
    subset="GB",
    data_path=base_config.ROOT_DATA_PATH,
    rel_data_path=base_config.RAW_DATA_PATH,
    usecols=base_config.EPC_FEAT_SELECTION,
    batch=None,
    n_samples=None,
    remove_duplicates=True,
    save_data=base_config.PREPROC_EPC_DATA_PATH,
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

    if save_data or not os.path.isabs(save_data):

        save_data = data_path / save_data

        if subset != "GB":
            warnings.warn("Careful! You're not loading the complete GB dataset.")

    epc_df = epc_data.load_raw_epc_data(
        subset=subset,
        data_path=data_path,
        rel_data_path=rel_data_path,
        batch=batch,
        usecols=usecols,
        n_samples=n_samples,
    )

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
        usecols=base_config.EPC_FEAT_SELECTION
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
            #  "LMK_KEY",
        ],
        n_samples=None,
        save_data=base_config.PREPROC_EPC_DATA_PATH,
        data_path=ASF_CORE_DATA_DIR,
    )

    end_time = time.time()
    runtime = round((end_time - start_time) / 60)

    print("\nLoading and preprocessing the EPC data took {} minutes.".format(runtime))

    print(epc_df.shape)
    print(epc_df.head())


if __name__ == "__main__":
    # Execute only if run as a script
    main()
