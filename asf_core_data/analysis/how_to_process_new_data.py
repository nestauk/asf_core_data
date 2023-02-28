import os

from asf_core_data import generate_and_save_mcs
from asf_core_data import load_preprocessed_epc_data
from asf_core_data.getters.epc import data_batches
from asf_core_data.pipeline.preprocessing import preprocess_epc_data
from asf_core_data.pipeline.data_joining import merge_install_dates

# %%
LOCAL_DATA_DIR = "/path/to/dir"

if not os.path.exists(LOCAL_DATA_DIR):
    os.makedirs(LOCAL_DATA_DIR)

# %%
# Check whether newest batch shows up a newest in local data dir
print("Local input dir\n---------------")
print(
    "Available batches:",
    data_batches.get_all_batch_names(data_path=LOCAL_DATA_DIR, check_folder="inputs"),
)
print("Newest batch:", data_batches.get_most_recent_batch(data_path=LOCAL_DATA_DIR))

# %%
# Process new batch of EPC data
epc_full = preprocess_epc_data.load_and_preprocess_epc_data(
    data_path=LOCAL_DATA_DIR, batch="newest", subset="GB", reload_raw=True
)

# %%
# Get MCS and join with MCS
generate_and_save_mcs()

# %%
# Load the processed EPC data
prep_epc = load_preprocessed_epc_data(
    data_path="S3",
    version="preprocessed",
    usecols=[
        "UPRN",
        "HEATING_SYSTEM",
        "HEATING_FUEL",
        "MAINHEAT_DESCRIPTION",
        "MAINS_GAS_FLAG",
        "INSPECTION_DATE",
        "HP_INSTALLED",
        "HP_TYPE",
        "PROPERTY_TYPE",
        "BUILT_FORM",
        "COUNTRY",
        "MAINS_GAS_FLAG",
        "LOCAL_AUTHORITY_LABEL",
        "COUNTRY",
        "CURRENT_ENERGY_RATING",
        "CONSTRUCTION_AGE_BAND",
        "NUMBER_HABITABLE_ROOMS",
        "TENURE",
        "POSTCODE",
    ],
    batch="newest",
)


# %%
# Add more precise estimations for heat pump installation dates via MCS data
epc_with_MCS_dates = merge_install_dates.manage_hp_install_dates(prep_epc)
epc_with_MCS_dates.to_csv("epc_with_hp_dates.csv")
