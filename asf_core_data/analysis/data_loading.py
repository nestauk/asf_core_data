# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     comment_magics: true
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.7
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## ASF Core Data Infrastructure -  Data Loaders
#
# This notebook shows how to load and use the EPC data and related datasets. <br>
# The data can be loaded directly from the S3 bucket named [`asf-core-data`](https://s3.console.aws.amazon.com/s3/buckets/asf-core-data?region=eu-west-2&tab=objects) or it can be downloaded to a local directory, from where it will be loaded when needed.

# %%
# %load_ext autoreload
# %autoreload 2


import os
import asf_core_data

from asf_core_data.getters.epc import epc_data, data_batches
from asf_core_data.getters.supplementary_data.deprivation import imd_data
from asf_core_data.getters.supplementary_data.geospatial import coordinates
from asf_core_data.pipeline.preprocessing import preprocess_epc_data
from asf_core_data.utils.visualisation import easy_plotting
from asf_core_data.getters import data_getters

from asf_core_data.config import base_config

from asf_core_data.pipeline.data_joining import merge_install_dates

from asf_core_data import Path

# %% [markdown]
# ## Local Data Dir
#
# While all the data can be loaded from S3 directly, some data takes a long time to load. It is easiest to keep a local copy in a designated folder that mirrors the structure of the S3 bucket asf-core-data.
#
# The setup is easy: create an empty folder of any name in any location you like. The next cell will even create this folder for you if it doesn't already exist.
#
# The keyword argument `data_path` that is passed to many loading functions defines whether the data is loaded from S3 (`"S3"`) or from the local directory (`LOCAL_DATA_DIR`). The advantage of loading from S3 is that you data is always up-to-date. The disadvantage is that reading certain data directly from S3 can take a long time, which would need to be repeated everytime the code is executed.
#
# We recommend using direct loading from S3 for smaller datasets, such as MCS and supplementary data or smaller subsets of the EPC data, but keep a copy of the large EPC files in a local directory. Functions such as `data_batches.check_for_newest_batch()` will notify you if your local batch is not up-to-date.
#
# **Note**: the keyword `data_path` will be renamed to `data_source` before the next merge.
#

# %%
LOCAL_DATA_DIR = "/path/to/dir"

if not os.path.exists(LOCAL_DATA_DIR):
    os.makedirs(LOCAL_DATA_DIR)

# %% [markdown]
# If you have just created a new data folder, then before downloading any data, there will be no EPC data batches in your local directory.
#

# %%
print("Local dir\n---------------")
print(
    "Available batches:",
    data_batches.get_all_batch_names(data_path=LOCAL_DATA_DIR, check_folder="inputs"),
)

# %% [markdown]
# ## Download some of the data to your local dir
#
# Our current data dir is still empty at this point. The function `print_download_options()` shows us what data can be downloaded from S3. The option of downloading MCS data will be added shortly.
#
# We download the raw EPC data for England, Wales and Scotland for the batch `2021_Q2_0721`. We also download the raw and the preprocessed + deduplicated EPC data of the newest batch.

# %%
data_getters.print_download_options()

# %%
# %%time
data_getters.download_core_data("epc_raw", LOCAL_DATA_DIR, batch="2021_Q2_0721")

# %%
# %%time
data_getters.download_core_data("epc_raw_combined", LOCAL_DATA_DIR, batch="newest")

# %%
# %%time
data_getters.download_core_data(
    "epc_preprocessed_dedupl", LOCAL_DATA_DIR, batch="newest"
)

# %% [markdown]
# ## Batch checking
#
# When working with local data, it is important to keep track of the batches and keep your data up-to-date. The first output shows that our input directory is not up-to-date but our output directory is. (Note that we didn't download the same batch for the raw and preprocessed data above.)
#
# After downloading the most recent data batch for the raw EPC data, we get confirmation that our data is now up-to-date. The batch checking functions show that our local input directory now contains two batches and the output directory contains only one batch.
#
# The most recent batch will always contain all the EPC data available to us (including previous batches), so the number of samples will naturally increase with every batch. The number of features may also change as new fields are added, although this may not happen with every new batch.

# %%
data_batches.check_for_newest_batch(
    data_path=LOCAL_DATA_DIR, check_folder="inputs", verbose=True
)

# %%
data_batches.check_for_newest_batch(
    data_path=LOCAL_DATA_DIR, check_folder="outputs", verbose=True
)

# %%
data_getters.download_core_data("epc_raw", LOCAL_DATA_DIR, batch="newest")

# %%
data_batches.check_for_newest_batch(
    data_path=LOCAL_DATA_DIR, check_folder="inputs", verbose=True
)

# %%
print("S3 bucket\n---------------")
print("Available batches:", data_batches.get_all_batch_names(data_path="S3"))
print("Newest batch:", data_batches.get_most_recent_batch(data_path="S3"))

# %%
print("Local input dir\n---------------")
print(
    "Available batches:",
    data_batches.get_all_batch_names(data_path=LOCAL_DATA_DIR, check_folder="inputs"),
)
print("Newest batch:", data_batches.get_most_recent_batch(data_path=LOCAL_DATA_DIR))

# %%
print("Local output dir\n---------------")
print(
    "Available batches:",
    data_batches.get_all_batch_names(data_path=LOCAL_DATA_DIR, check_folder="outputs"),
)
print("Newest batch:", data_batches.get_most_recent_batch(data_path=LOCAL_DATA_DIR))

# %%
wales_epc = epc_data.load_england_wales_data(
    data_path=LOCAL_DATA_DIR, subset="Wales", batch="2021_Q2_0721"
)
print(
    "Batch 2021_Q2_0721 includes {} samples and {} features.".format(
        wales_epc.shape[0], wales_epc.shape[1]
    )
)

# %%
wales_epc = epc_data.load_england_wales_data(
    data_path=LOCAL_DATA_DIR, subset="Wales", batch="newest"
)
print(
    "Batch 2021_Q4_0721 includes {} samples and {} features.".format(
        wales_epc.shape[0], wales_epc.shape[1]
    )
)

# %% [markdown]
# ## Loading raw EPC data
#
# ### Loading from local dir
#
#
# Load different subsets and batches of raw EPC data from the local data dir.

# %%
wales_epc = epc_data.load_raw_epc_data(
    data_path=LOCAL_DATA_DIR, subset="Wales", batch="newest"
)
wales_epc.shape

# %%
wales_recs = epc_data.load_england_wales_data(
    data_path=LOCAL_DATA_DIR, subset="Wales", load_recs=True
)
wales_recs.columns

# %%
scotland_epc = epc_data.load_raw_epc_data(
    data_path=LOCAL_DATA_DIR, subset="Scotland", batch="2021_Q2_0721"
)
scotland_epc.shape

# %%
scotland_epc = epc_data.load_raw_epc_data(
    data_path=LOCAL_DATA_DIR, subset="Scotland", batch="newest"
)
scotland_epc.shape

# %% [markdown]
# ### Loading directly from S3
#
# This works, but it can take a while and is not recommended for frequent use.
#
# Loading the Wales EPC data can take up to 5min. <br>
# Loading the Scotland EPC data can take up to 10min. <br>
# Loading the England EPC data can take up to 30min. <br>
# Loading the entire GB EPC data takes even longer... <br>
#
# It is recommended to download the data and read the data from a local directory instead, especially if needed more than just once.

# %%
# # Commented out to reduce runtime

# england_epc = epc_data.load_raw_epc_data(
#     data_path=LOCAL_DATA_DIR, subset="England", batch="newest"
# )
# england_epc.shape

# %%
# # Commented out to reduce runtime

# # %%time
# epc_data.load_raw_epc_data(
#     data_path="S3", subset="Wales")

# %%
# #Commented out to reduce runtime

# # %%time
# epc_data.load_raw_epc_data(
#     data_path="S3", subset="Scotland")

# %%
# # Commented out to reduce runtime

# # %%time
# epc_data.load_raw_epc_data(
#     data_path="S3", subset="England")

# %%
# # Commented out to reduce runtime

# # %%time
# epc_data.load_raw_epc_data(
#     data_path="S3", subset="GB")

# %% [markdown]
# ## Load preprocessed EPC data
#
# ### Loading from local dir

# %%
prep_epc = epc_data.load_preprocessed_epc_data(
    data_path=LOCAL_DATA_DIR,
    version="raw",
    usecols=[
        "UPRN",
        "CURRENT_ENERGY_RATING",
        "INSPECTION_DATE",
        "PROPERTY_TYPE",
        "CONSTRUCTION_AGE_BAND",
    ],
)

# %%
prep_epc = epc_data.load_preprocessed_epc_data(
    data_path=LOCAL_DATA_DIR,
    version="preprocessed_dedupl",
    usecols=[
        "UPRN",
        "CURRENT_ENERGY_RATING",
        "INSPECTION_DATE",
        "PROPERTY_TYPE",
        "CONSTRUCTION_AGE_BAND",
    ],
    n_samples=5000,
)

# %% [markdown]
# ### Loading data from directly from S3

# %%
prep_epc = epc_data.load_preprocessed_epc_data(
    data_path="S3",
    version="preprocessed_dedupl",
    usecols=[
        "UPRN",
        "CURRENT_ENERGY_RATING",
        "INSPECTION_DATE",
        "PROPERTY_TYPE",
        "CONSTRUCTION_AGE_BAND",
    ],
    batch="2021_Q4_0721",
    n_samples=5000,
)

# %% [markdown]
# Pro tip: you can filter the data by entry year.

# %%
prep_epc_2015 = epc_data.filter_by_year(prep_epc, 2015, up_to=True)
sorted(prep_epc_2015["INSPECTION_DATE"].dt.year.unique())

# %% [markdown]
# ### Preprocess the data
#
# As preprocessed versions are available on S3 for each batch, this functionionality is optional. Preprocessing can be repeated when the preprocessing pipeline has been updated or if further fields are added.

# %%
wales_epc_reduced = preprocess_epc_data.load_and_preprocess_epc_data(
    data_path=LOCAL_DATA_DIR, batch="2021_Q2_0721", subset="Wales"
)

# %%
wales_epc_with_MCS_dates = merge_install_dates.manage_hp_install_dates(
    wales_epc_reduced
)
wales_epc_with_MCS_dates.head()

# %%
easy_plotting.plot_subcats_by_other_subcats(
    wales_epc_with_MCS_dates,
    "PROPERTY_TYPE",
    "CONSTRUCTION_AGE_BAND",
    plotting_colors="viridis",
    legend_loc="outside",
    plot_title="Construction Age by Property Type",
    fig_save_path=LOCAL_DATA_DIR,
)

# %% [markdown]
# ## Supplementary data
#
# Most supplementary data is small enough that it can be loaded from S3 directly if needed, but you can also download all of it to a local dir.

# %%
imd_df = imd_data.get_gb_imd_data(data_path="S3")
imd_df["Country"].value_counts()

# %%
imd_data.get_country_imd_data("Wales", data_path="S3")

# %%
coords = coordinates.get_postcode_coordinates()
coords.head()

# %%
data_getters.download_core_data("supplementary_data", LOCAL_DATA_DIR)

# %%
imd_df = imd_data.get_gb_imd_data(data_path=LOCAL_DATA_DIR)
imd_df["Country"].value_counts()

# %% [markdown]
# ## EST cleansed version
#
# This data is probably no longer needed but could be loaded as follows.
# They are very large files and it would be hard to read it directly from S3.

# %%
data_getters.download_core_data("EST_cleansed_dedupl", LOCAL_DATA_DIR)

# %%
est_cleansed = epc_data.load_cleansed_epc(data_path=LOCAL_DATA_DIR, n_samples=5)
est_cleansed.head()

# %%
