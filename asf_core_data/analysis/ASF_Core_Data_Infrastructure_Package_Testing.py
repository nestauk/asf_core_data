# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     comment_magics: true
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: core_package_testing
#     language: python
#     name: core_package_testing
# ---

# %% [markdown]
# ## ASF Core Data Infrastructure Testing
#
#

# %%
# %load_ext autoreload
# %autoreload 2

import asf_core_data

from asf_core_data.getters.epc import epc_data, data_batches
from asf_core_data.getters.supplementary_data.deprivation import imd_data
from asf_core_data.getters.supplementary_data.geospatial import coordinates
from asf_core_data.pipeline.preprocessing import preprocess_epc_data
from asf_core_data.utils.visualisation import easy_plotting

from asf_core_data.pipeline.data_joining import merge_install_dates

from asf_core_data import Path

# %%
# Enter the path to your local data dir
# Adjust data dir!!
LOCAL_DATA_DIR = "/Users/chris.williamson/ASF_data"

# %%
data_batches.check_for_newest_batch(data_path=LOCAL_DATA_DIR, verbose=True)

# %%
print("S3 bucket\n---------------")
print("Available batches:", data_batches.get_all_batch_names(data_path="S3"))
print("Newest batch:", data_batches.get_most_recent_batch(data_path="S3"))

# %%
print("S3 bucket\n---------------")
print("Available batches:", data_batches.get_all_batch_names(data_path=LOCAL_DATA_DIR))
print("Newest batch:", data_batches.get_most_recent_batch(data_path=LOCAL_DATA_DIR))

# %%
england_epc = epc_data.load_england_wales_data(
    data_path=LOCAL_DATA_DIR, subset="England", batch="2021_Q2_0721"
)
print(
    "Batch 2021_Q2_0721 includes {} samples and {} features.".format(
        england_epc.shape[0], england_epc.shape[1]
    )
)

# %%
wales_epc = epc_data.load_england_wales_data(
    data_path=LOCAL_DATA_DIR, subset="Wales", batch="2021_Q4_0721"
)
print(
    "Batch 2021_Q4_0721 includes {} samples and {} features.".format(
        wales_epc.shape[0], wales_epc.shape[1]
    )
)

# %%
wales_epc = epc_data.load_raw_epc_data(
    data_path=LOCAL_DATA_DIR, subset="Wales", batch="newest"
)
wales_epc.shape

# %%
scotland_epc = epc_data.load_raw_epc_data(
    data_path=LOCAL_DATA_DIR, subset="Scotland", batch="newest"
)
scotland_epc.shape

# %%
wales_epc_reduced = preprocess_epc_data.load_and_preprocess_epc_data(
    data_path=LOCAL_DATA_DIR, subset="Wales", batch="newest", n_samples=1500
)

# %%
wales_epc = preprocess_epc_data.load_and_preprocess_epc_data(
    data_path=LOCAL_DATA_DIR, subset="Wales", batch="newest"
)

# %%
# Takes a while!
# Run only if you have the data 'EPC_GB_preprocessed_and_deduplicated' at hand,
# no need to download it just for testing this line of code!
# You can also simply re-name 'EPC_Wales_preprocessed_and_deduplicated' which was created in the previous cell

eng_epc = epc_data.load_preprocessed_epc_data(
    data_path=LOCAL_DATA_DIR,
    subset="GB",
    batch="newest",
    usecols=["ADDRESS1", "POSTCODE", "MAINHEAT_DESCRIPTION", "ENERGY_TARIFF"],
)
eng_epc.shape

# %%
wales_epc_2015 = epc_data.filter_by_year(wales_epc, 2015, up_to=True)
print(wales_epc_2015.shape)

wales_epc_2015["INSPECTION_DATE"].dt.year.unique()

# %%
wales_epc = preprocess_epc_data.load_and_preprocess_epc_data(
    data_path=LOCAL_DATA_DIR, subset="Wales", batch="newest", n_samples=1500
)

wales_epc_with_MCS_dates = merge_install_dates.manage_hp_install_dates(
    wales_epc_reduced
)
wales_epc_with_MCS_dates.head()

# %%
imd_df = imd_data.get_gb_imd_data(data_path=LOCAL_DATA_DIR)
imd_df["Country"].value_counts()

# %%
coord_df = coordinates.get_postcode_coordinates(data_path=LOCAL_DATA_DIR)
coord_df.head()

# %%
easy_plotting.plot_subcats_by_other_subcats(
    wales_epc,
    "PROPERTY_TYPE",
    "CONSTRUCTION_AGE_BAND",
    Path(LOCAL_DATA_DIR) / "outputs/figures/",
    plotting_colors="viridis",
    legend_loc="outside",
    plot_title="Construction Age by Property Type",
)

# %%
