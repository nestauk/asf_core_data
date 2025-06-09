# %% [markdown]
# ## Compare MCS processing
#
# This notebook uses Y-Data profiling to compare the output MCS installations; MCS installers and MCS-EPC full joined datasets from the `asf_core_data` and `asf_daps` pipelines. We also compare datasets row by row to identify any differences between the datasets.

# %%
import pandas as pd
import polars as pl
from ydata_profiling import ProfileReport
import unittest
import matplotlib.pyplot as plt

# %% [markdown]
# ## MCS installations data

# %%
# Import data
dataset = "mcs_installations"

core_mcs_path = "s3://asf-core-data/outputs/MCS/mcs_installations_250527.csv"
daps_mcs_path = (
    "s3://asf-daps/lakehouse/2024_Q4/processed/mcs/mcs_installations_250529-0.parquet"
)

core_df = pd.read_csv(core_mcs_path)
daps_df = pd.read_parquet(daps_mcs_path).astype({"commission_date": "string"})

# %%
# Compare with y-data profiling
core_report = ProfileReport(core_df, title=f"Core {dataset.upper()}", minimal=True)
daps_report = ProfileReport(daps_df, title=f"Daps {dataset.upper()}", minimal=True)
comparison_report = core_report.compare(daps_report)
comparison_report.to_file(f"{dataset}_comparison.html")

# %%
# Get row by row diffs between dataframes

# We need to fill nulls because two nulls do not evaluate to equal
diffs_df = pl.from_pandas(
    (core_df.fillna("").sort_index() != daps_df.fillna("").sort_index())
)
diffs_T = diffs_df.sum().transpose(
    include_header=True, header_name="feature", column_names=["diffs_count"]
)
diffs_T = diffs_T.with_columns(
    (pl.col("diffs_count") / len(diffs_df) * 100).alias("percent")
)

print("Diffs counts per column:")
print(diffs_T.sort("diffs_count", descending=True))

print("Columns with no diffs:")
print(diffs_T.filter(pl.col("diffs_count") == 0))

print("Columns with diffs:")
print(diffs_T.filter(pl.col("diffs_count") != 0))

# %% [markdown]
# ## MCS installers datasets

# %%
# Import data
dataset = "mcs_installers"

core_mcs_path = (
    "s3://asf-core-data/outputs/MCS/installers/mcs_historical_installers_20250430.csv"
)
daps_mcs_path = (
    "s3://asf-daps/lakehouse/2024_Q4/processed/mcs/mcs_installers_250529-0.parquet"
)

core_df = pd.read_csv(core_mcs_path)
daps_df = pd.read_parquet(daps_mcs_path)
core_df = core_df.astype(daps_df.dtypes.to_dict())

assert (core_df.dtypes == daps_df.dtypes).all()

assert len(core_df) == len(daps_df)
print(len(core_df))

assert core_df.shape == daps_df.shape
print(core_df.shape)

assert (core_df.columns == daps_df.columns).all()

# %%
# Compare with y-data profiling
core_report = ProfileReport(core_df, title=f"Core {dataset.upper()}", minimal=True)
daps_report = ProfileReport(daps_df, title=f"Daps {dataset.upper()}", minimal=True)
comparison_report = core_report.compare(daps_report)
comparison_report.to_file(f"{dataset}_comparison.html")

# %%
# Get row by row diffs between dataframes

# Convert datetime columns to string
dt_columns = daps_df.select_dtypes(include=["datetime"]).columns
# We need to fill nulls because two nulls do not evaluate to equal
diffs_df = pl.from_pandas(
    (
        core_df.astype({col: "string" for col in dt_columns}).fillna("").sort_index()
        != daps_df.astype({col: "string" for col in dt_columns}).fillna("").sort_index()
    )
)

diffs_T = diffs_df.sum().transpose(
    include_header=True, header_name="feature", column_names=["diffs_count"]
)
diffs_T = diffs_T.with_columns(
    (pl.col("diffs_count") / len(diffs_df) * 100).alias("percent")
)

print("Diffs counts per column:")
print(diffs_T.sort("diffs_count", descending=True))

print("Columns with no diffs:")
print(diffs_T.filter(pl.col("diffs_count") == 0))

print("Columns with diffs:")
print(diffs_T.filter(pl.col("diffs_count") != 0))

# %%
# Plot count of diffs per feature
plot_data = diffs_T.filter(pl.col("diffs_count") > 0).sort(
    "diffs_count", descending=True
)
x = plot_data["feature"]
y = plot_data["diffs_count"].cast(pl.Float64)

fig, ax = plt.subplots(figsize=(8, 5))
ax.bar(x=x, height=y)
plt.xticks(rotation=90)
plt.title(
    "Count of rows with differences between core and daps\noutputs by feature (MCS installers Q4 2024)"
)
plt.ylabel("Row count")
plt.show()

# %% [markdown]
# ## MCS-EPC full join datasets

# %%
# Import data
dataset = "mcs_epc_full"

core_epc_path = "s3://asf-core-data/outputs/MCS/mcs_installations_epc_full_250527.csv"
daps_epc_path = "s3://asf-daps/lakehouse/2024_Q4/processed/mcs/mcs_installations_epc_full_250529-0.parquet"

raw_core_df = pd.read_csv(core_epc_path)
raw_daps_df = pd.read_parquet(daps_epc_path)

# %%
# Preprocess datasets to make them comparable

# Convert to same schema as core
schema = {
    k: v for k, v in raw_daps_df.dtypes.to_dict().items() if k in raw_core_df.columns
}
core_df = raw_core_df.astype(schema)

dt_columns = raw_daps_df.select_dtypes(include=["datetime", "category"])

daps_df = raw_daps_df.astype({k: "string" for k in dt_columns})
core_df = core_df.astype({k: "string" for k in dt_columns})

daps_df["INSPECTION_DATE"] = pd.to_datetime(daps_df["INSPECTION_DATE"]).astype(str)
core_df["INSPECTION_DATE"] = pd.to_datetime(core_df["INSPECTION_DATE"]).astype(str)

# %%
# Compare with y-data profiling
core_report = ProfileReport(core_df, title=f"Core {dataset.upper()}")
daps_report = ProfileReport(daps_df, title=f"Daps {dataset.upper()}")
comparison_report = core_report.compare(daps_report)
comparison_report.to_file(f"{dataset}_comparison.html")

# %%
# Identify diffs in df shape
print("Number of columns in core:")
print(len(core_df.columns))
print("Number of columns in daps:")
print(len(daps_df.columns))


print("Columns in core and not in daps:")
print(set(core_df.columns) - set(daps_df.columns))
print("Columns in daps and not in core:")
print(set(daps_df.columns) - set(core_df.columns))

# %%
# Create unique ID to prepare data for row by row comparison

# Drop any rows with NaN values in any unique ID col
clean_core_df = core_df.dropna(subset=["commission_date", "INSPECTION_DATE", "UPRN"])
clean_daps_df = daps_df.dropna(subset=["commission_date", "INSPECTION_DATE", "UPRN"])

# Ensure UPRN are in the same format
clean_core_df["UPRN"] = clean_core_df["UPRN"].apply(
    lambda x: x.split(".0")[0] if x.endswith(".0") else x
)
clean_core_df["UPRN"] = clean_core_df["UPRN"].str.replace(" unknown", "")
clean_daps_df["UPRN"] = clean_daps_df["UPRN"].apply(
    lambda x: x.split(".0")[0] if x.endswith(".0") else x
)

# Concatenate values to generate unique ID
clean_core_df["unique_id"] = (
    clean_core_df["commission_date"].astype(str)
    + clean_core_df["INSPECTION_DATE"].astype(str)
    + clean_core_df["UPRN"].astype(str)
)
clean_daps_df["unique_id"] = (
    clean_daps_df["commission_date"].astype(str).str[:10]
    + clean_daps_df["INSPECTION_DATE"].astype(str)
    + clean_daps_df["UPRN"].astype(str)
)

# %%
# Preprocessing for row by row comparison

print("% of duplicated rows in core:")
print(clean_core_df["unique_id"].duplicated().sum() / len(clean_core_df) * 100)

print("% of duplicated rows in daps:")
print(clean_daps_df["unique_id"].duplicated().sum() / len(clean_daps_df) * 100)

# Drop all duplicated IDs because we can't compare them easily
clean_core_df = clean_core_df.drop_duplicates(subset=["unique_id"], keep=False)
clean_daps_df = clean_daps_df.drop_duplicates(subset=["unique_id"], keep=False)

# Identify IDs which are in both datasets
shared_indices = set(clean_core_df.unique_id).intersection(clean_daps_df.unique_id)
print("Count of shared unique IDs:")
print(len(shared_indices))
print("% of rows retained after filtering in core:")
print(len(shared_indices) / len(core_df) * 100)
print("% of rows retained after filtering in daps:")
print(len(shared_indices) / len(daps_df) * 100)

# Identify columns which are in both datasets
shared_columns = list(set(clean_core_df.columns).intersection(clean_daps_df.columns))
print("Number of shared columns:")
print(len(shared_columns))

# %%
# Create row by row diffs
diffs_df = pl.from_pandas(
    clean_core_df[clean_core_df.unique_id.isin(shared_indices)][shared_columns]
    .fillna("")
    .sort_values(by="unique_id")
    .reset_index(drop=True)
    != clean_daps_df[clean_daps_df.unique_id.isin(shared_indices)][shared_columns]
    .fillna("")
    .sort_values(by="unique_id")
    .reset_index(drop=True)
)

diffs_T = diffs_df.sum().transpose(
    include_header=True, header_name="feature", column_names=["diffs_count"]
)
diffs_T = diffs_T.with_columns(
    (pl.col("diffs_count") / len(diffs_df) * 100).alias("percent")
)

print("Diffs counts per column:")
print(diffs_T.sort("diffs_count", descending=True))

print("Columns with no diffs:")
print(diffs_T.filter(pl.col("diffs_count") == 0))

print("Columns with diffs:")
print(diffs_T.filter(pl.col("diffs_count") != 0))

print("Number of columns with diffs:")
print(len(diffs_T.filter(pl.col("diffs_count") != 0)))
print("% of columns with diffs:")
print(len(diffs_T.filter(pl.col("diffs_count") != 0)) / len(diffs_T))

# %%
# Plot count of diffs per feature
plot_data = diffs_T.filter(pl.col("diffs_count") > 0).sort(
    "diffs_count", descending=True
)
x = plot_data["feature"]
y = plot_data["diffs_count"].cast(pl.Float64)

fig, ax = plt.subplots(figsize=(8, 5))
ax.bar(x=x, height=y)
ax.set_yscale("log")
plt.xticks(rotation=90)
plt.title(
    "Log count of rows with differences between core and daps\noutputs by feature (MCS-EPC joined Q4 2024)"
)
plt.ylabel("Row count (log)")
plt.show()

# %% [markdown]
# ## MCS-EPC most relevant join datasets

# %%
# Import data
dataset = "mcs_epc_most_relevant"

core_epc_path = (
    "s3://asf-core-data/outputs/MCS/mcs_installations_epc_most_relevant_250527.csv"
)
daps_epc_path = "s3://asf-daps/lakehouse/2024_Q4/processed/mcs/mcs_installations_epc_dedupl_most_relevant_250529-0.parquet"

raw_core_df = pd.read_csv(core_epc_path)
raw_daps_df = pd.read_parquet(daps_epc_path)

# %%
# Preprocess datasets to make them comparable

# Convert to same schema as core
schema = {
    k: v for k, v in raw_daps_df.dtypes.to_dict().items() if k in raw_core_df.columns
}
core_df = raw_core_df.astype(schema)

dt_columns = raw_daps_df.select_dtypes(include=["datetime", "category"])

daps_df = raw_daps_df.astype({k: "string" for k in dt_columns})
core_df = core_df.astype({k: "string" for k in dt_columns})

daps_df["INSPECTION_DATE"] = pd.to_datetime(daps_df["INSPECTION_DATE"]).astype(str)
core_df["INSPECTION_DATE"] = pd.to_datetime(core_df["INSPECTION_DATE"]).astype(str)

# %%
# Compare with y-data profiling
core_report = ProfileReport(core_df, title=f"Core {dataset.upper()}")
daps_report = ProfileReport(daps_df, title=f"Daps {dataset.upper()}")
comparison_report = core_report.compare(daps_report)
comparison_report.to_file(f"{dataset}_comparison.html")

# %%
# Identify diffs in df shape
print("Number of columns in core:")
print(len(core_df.columns))
print("Number of columns in daps:")
print(len(daps_df.columns))

print("Columns in core and not in daps:")
print(set(core_df.columns) - set(daps_df.columns))
print("Columns in daps and not in core:")
print(set(daps_df.columns) - set(core_df.columns))

# %%
# Create unique ID to prepare data for row by row comparison

# Drop any rows with NaN values in any unique ID col
clean_core_df = core_df.dropna(subset=["commission_date", "INSPECTION_DATE", "UPRN"])
clean_daps_df = daps_df.dropna(subset=["commission_date", "INSPECTION_DATE", "UPRN"])

# Ensure UPRN are in the same format
clean_core_df["UPRN"] = clean_core_df["UPRN"].apply(
    lambda x: x.split(".0")[0] if x.endswith(".0") else x
)
clean_core_df["UPRN"] = clean_core_df["UPRN"].str.replace(" unknown", "")
clean_daps_df["UPRN"] = clean_daps_df["UPRN"].apply(
    lambda x: x.split(".0")[0] if x.endswith(".0") else x
)

# Concatenate values to generate unique ID
clean_core_df["unique_id"] = (
    clean_core_df["commission_date"].astype(str)
    + clean_core_df["INSPECTION_DATE"].astype(str)
    + clean_core_df["UPRN"].astype(str)
)
clean_daps_df["unique_id"] = (
    clean_daps_df["commission_date"].astype(str).str[:10]
    + clean_daps_df["INSPECTION_DATE"].astype(str)
    + clean_daps_df["UPRN"].astype(str)
)

# %%
# Preprocessing for row by row comparison

print("% of duplicated rows in core:")
print(clean_core_df["unique_id"].duplicated().sum() / len(clean_core_df) * 100)

print("% of duplicated rows in daps:")
print(clean_daps_df["unique_id"].duplicated().sum() / len(clean_daps_df) * 100)

# Drop all duplicated IDs because we can't compare them easily
clean_core_df = clean_core_df.drop_duplicates(subset=["unique_id"], keep=False)
clean_daps_df = clean_daps_df.drop_duplicates(subset=["unique_id"], keep=False)

# Identify IDs which are in both datasets
shared_indices = set(clean_core_df.unique_id).intersection(clean_daps_df.unique_id)
print("Count of shared unique IDs:")
print(len(shared_indices))
print("% of rows retained after filtering in core:")
print(len(shared_indices) / len(core_df) * 100)
print("% of rows retained after filtering in daps:")
print(len(shared_indices) / len(daps_df) * 100)

# Identify columns which are in both datasets
shared_columns = list(set(clean_core_df.columns).intersection(clean_daps_df.columns))
print("Number of shared columns:")
print(len(shared_columns))

# %%
# Create row by row diffs
diffs_df = pl.from_pandas(
    clean_core_df[clean_core_df.unique_id.isin(shared_indices)][shared_columns]
    .fillna("")
    .sort_values(by="unique_id")
    .reset_index(drop=True)
    != clean_daps_df[clean_daps_df.unique_id.isin(shared_indices)][shared_columns]
    .fillna("")
    .sort_values(by="unique_id")
    .reset_index(drop=True)
)

diffs_T = diffs_df.sum().transpose(
    include_header=True, header_name="feature", column_names=["diffs_count"]
)
diffs_T = diffs_T.with_columns(
    (pl.col("diffs_count") / len(diffs_df) * 100).alias("percent")
)

print("Diffs counts per column:")
print(diffs_T.sort("diffs_count", descending=True))

print("Columns with no diffs:")
print(diffs_T.filter(pl.col("diffs_count") == 0))

print("Columns with diffs:")
print(diffs_T.filter(pl.col("diffs_count") != 0))

print("Number of columns with diffs:")
print(len(diffs_T.filter(pl.col("diffs_count") != 0)))
print("% of columns with diffs:")
print(len(diffs_T.filter(pl.col("diffs_count") != 0)) / len(diffs_T))

# %%
# Plot count of diffs per feature
plot_data = diffs_T.filter(pl.col("diffs_count") > 0).sort(
    "diffs_count", descending=True
)
x = plot_data["feature"]
y = plot_data["diffs_count"].cast(pl.Float64)

fig, ax = plt.subplots(figsize=(8, 5))
ax.bar(x=x, height=y)
ax.set_yscale("log")
plt.xticks(rotation=90)
plt.title(
    "Log count of rows with differences between core and daps\noutputs by feature (MCS-EPC joined Q4 2024)"
)
plt.ylabel("Row count (log)")
plt.show()

# %%
