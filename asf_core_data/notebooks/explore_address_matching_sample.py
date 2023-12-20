# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     comment_magics: true
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: asf_core_data_kernel
#     language: python
#     name: asf_core_data_kernel
# ---

# %% [markdown]
# ### Methodology for manually reviewing address matching sample
#
# 1002 matched addresses were manually reviewed to assess whether we can confidently say the match is correct or not. The methodology followed in this process is outlined below:
# - mcs_address and matched epc_address for each row were manually reviewed.
# - Addresses that were confidently deemed to be the referring to the same address were assigned a confident_match of 1. Clear mismatches were assigned a confident_match of 0.
# - Addresses where there was enough difference to create uncertainty were checked in the Royal Mail Address Finder (https://www.royalmail.com/find-a-postcode) by searching the postcode and common words/numeric tokens across both addresses.
# - If, after checking, there was no ambiguity that mcs_address and epc_address referred to the same address, addresses would be assigned a confident_match of 1. If ambiguity remained, addresses would be assigned a confident_match of 0.
#
# #### Illustrative example with fictitious address:
# mcs_address == 'example farm'\
# epc_address == 'example farm house'\
# postcode == 'POST1'
#
# We search 'POST1 example farm' in Royal Mail address finder:\
# Scenario 1: Royal Mail address finder shows the following addresses at 'POST1' postcode:
# - 'example farm'
# - 'example farm barn'
#
# We cannot be confident which address 'example farm house' refers to so this pairing is given a confident_match of 0. This methodology should be kept in mind when reviewing the results.
#
# Scenario 2: Royal Mail address finder shows the following addresses at 'POST1' postcode:
# - 'example farm'
#
# As there is only 1 address at the postcode containing 'example farm', we assume the match is correct and the pairing is given a confident_match of 1.
#
# #### Caveats
# - For ease of reading, we refer in the following analysis to address matches with a confident_match score of 0 as errors. As you can see from the methodology, it is possible that some of these cases are actually true matches but we do not have the data to confidently confirm it. It is therefore possible that this methodology overestimates false positives.
# - It is also possible that this methodology underestimates false positives, which may occur if addresses have been incompletely or erroneously entered into the original datasets. For example, we may see 'example farm' in both the mcs_address and epc_address fields, but it is possible that one address could really refer to 'example farm' and the other to 'example farm barn'. However, we cannot assess for these possibilities in this analysis. Perhaps it would be possible with geographic data but we do not have that.
# - This methodology can only account for false positives and not false negatives (i.e. addresses that are true matches but have not been matched by the algorithm).

# %%
import os
import pandas as pd
import numpy as np

from statsmodels.stats.proportion import proportion_confint
import seaborn as sns
import matplotlib.pyplot as plt

from sklearn.metrics import roc_curve
from sklearn.metrics import RocCurveDisplay as rcd

# %% [markdown]
# ## Set params to retrieve reviewed sample

# %%
## Params

## Set your sample params
date = "20231218"
country_sample_size = 334
seed = 7

# %% [markdown]
# ## Read in reviewed address matching sample
# After manually checking through the sampled rows of matching addresses, we will assess the accuracy of address matching for each country.

# %%
mcs_epc = pd.read_csv(
    f"s3://asf-heat-pump-suitability/address-matching/{date}_mcs_epc_address_join_23Q2_prepr_dedupl_231009.csv",
    keep_default_na=False,
)
sample = pd.read_csv(
    f"s3://asf-heat-pump-suitability/address-matching/{date}_mcs_epc_address_matching_sample_n{country_sample_size*3}_seed{seed}_reviewed.csv",
    index_col=0,
)

# %% [markdown]
# ## Calculate address-matching error rate by country and estimate confidence intervals
#
# Here we calculate the error rate (of false positive errors) in address matching for each country in the sample. This represents the error rate (of false positives) of the address matching algorithm because we are only looking at addresses which are non-identical that were matched together. Therefore this tests the effectiveness of the fuzzy matching.
#
# We will also calculate the confidence intervals of the error using the Wilson interval. The Wilson interval has been selected as it is recommended for real data applications, https://www.scirp.org/pdf/ojs_2021101415023495.pdf .

# %%
## Error count in sample
errors = len(sample[sample["confident_match"] != 1])
print(f"Total errors in sample: {errors}")
print(f"Sample size: {len(sample)}")
print(f"% errors in sample: {round(100 * errors/len(sample), 2)}%")

# %%
## Calculate error rate by country

sample_country_split = (
    sample.groupby(["country"])["confident_match"]
    .value_counts()
    .reset_index(name="counts")
)
sample_country_split.groupby("country")["counts"].sum()
sample_country_split["error_sample"] = (
    sample_country_split["counts"] / country_sample_size
)
sample_country_split = sample_country_split[
    sample_country_split["confident_match"] == 0
]

# %%
## Calculate confidence intervals for error rates at low and higher bounds in the sample
## Wilson interval selected as it's recommended for real data applications
method = "wilson"
sample_country_split[f"{method}_confidence_interval"] = sample_country_split.apply(
    lambda row: proportion_confint(
        count=row["counts"], nobs=country_sample_size, alpha=0.05, method=method
    ),
    axis=1,
)

sample_country_split[f"{method}_lower_error"] = sample_country_split[
    f"{method}_confidence_interval"
].str[0]
sample_country_split[f"{method}_upper_error"] = sample_country_split[
    f"{method}_confidence_interval"
].str[1]

# %%
sample_country_split = sample_country_split[
    ["country", f"{method}_lower_error", "error_sample", f"{method}_upper_error"]
]
sample_country_split

# %% [markdown]
# The table above shows that the error rate is high. These numbers may also underestimate the true error rate because we cannot account for false negatives in our assessment.

# %% [markdown]
# ## Estimate overall population error rate
#
# Here we will use the error rates we have calculated to estimate the overall number/rate of false positive errors in our population dataset. We will include matches between identical addresses here to estimate overall numbers.
#
# We will also calculate confidence intervals for the overall error rate.

# %%
## View country split in population dataset

country_split = mcs_epc["country"].value_counts(dropna=False).reset_index(name="counts")
country_split["proportion"] = country_split["counts"] / country_split["counts"].sum()
country_split

# %%
## Overall error rate must take into account different country proportions in our population dataset
## Calculate confidence intervals for population error rate

## Take total count of non-exact matches in population dataset
country_totals = (
    mcs_epc.groupby(["country", "exact_match"])
    .size()
    .reset_index(name="non_exact_match_counts_popn")
)
country_totals = country_totals[country_totals["exact_match"] != 1]
country_totals = country_totals.drop(columns="exact_match")

## Merge dfs
popn_error_conf_int = country_totals.merge(
    sample_country_split, how="left", on="country"
)

# %%
## Get number of errors across conf int for each country
popn_error_conf_int["num_errors_popn"] = (
    popn_error_conf_int["error_sample"]
    * popn_error_conf_int["non_exact_match_counts_popn"]
)
popn_error_conf_int["num_lower_errors_popn"] = (
    popn_error_conf_int[f"{method}_lower_error"]
    * popn_error_conf_int["non_exact_match_counts_popn"]
)
popn_error_conf_int["num_upper_errors_popn"] = (
    popn_error_conf_int[f"{method}_upper_error"]
    * popn_error_conf_int["non_exact_match_counts_popn"]
)

## Add in total counts of address matches (including identical) by country across population
popn_error_conf_int = popn_error_conf_int.merge(
    mcs_epc.groupby("country").size().reset_index(name="total_counts_popn"),
    how="left",
    on="country",
)

## Get rate of errors across conf int for each country
popn_error_conf_int["rate_errors_popn"] = (
    popn_error_conf_int["num_errors_popn"] / popn_error_conf_int["total_counts_popn"]
)
popn_error_conf_int["rate_lower_errors_popn"] = (
    popn_error_conf_int["num_lower_errors_popn"]
    / popn_error_conf_int["total_counts_popn"]
)
popn_error_conf_int["rate_upper_errors_popn"] = (
    popn_error_conf_int["num_upper_errors_popn"]
    / popn_error_conf_int["total_counts_popn"]
)

# %%
## Split into 2 dfs for display and use
popn_conf_int_rate = popn_error_conf_int[
    ["country", "rate_lower_errors_popn", "rate_errors_popn", "rate_upper_errors_popn"]
]
popn_conf_int_num = popn_error_conf_int[
    ["country", "num_lower_errors_popn", "num_errors_popn", "num_upper_errors_popn"]
]

# %%
## View rate of predicted errors by country
popn_conf_int_rate.head()

# %%
## View number of predicted errors by country
popn_conf_int_num.head()

# %%
## Calculate overall confidence interval error rates

popn_error_rates = []

for e in ["num_lower_errors_popn", "num_errors_popn", "num_upper_errors_popn"]:
    total_est_errors = popn_conf_int_num[e].sum()
    popn_error_rates.append(round((total_est_errors / len(mcs_epc)) * 100, 2))

# %%
total_est_errors = popn_conf_int_num["num_errors_popn"].sum()
print(
    f"Sample size: {len(sample)}\n\
% of population sampled: {round(100*(len(sample)/len(mcs_epc)), 2)}%\n\
Size of population dataset: {len(mcs_epc)}\n\
Estimated number of false positives in population dataset: {round(total_est_errors)}\n\
Estimated % of false positives in population dataset: {round((total_est_errors/len(mcs_epc)) *100, 2)}%\n\
Wilson interval for error rates at 95% confidence: {popn_error_rates[0]}%-{popn_error_rates[2]}%"
)

# %% [markdown]
# Here we have calculated the lower and upper bounds of the estimated % of errors in the population level dataset at 95% confidence. However, as many addresses are exact matches, the error rate of the address matching algorithm is actually much higher (as we saw earlier) and requires further investigation.

# %% [markdown]
# ## Investigate addresses matched to NaN
# In the sample, there is one erroneous match between an address and a NaN value. Here we will look at why this occurred and how often it occurs across the whole population dataset.

# %%
mcs_epc_na = mcs_epc.loc[
    (mcs_epc["mcs_address"] == "") | (mcs_epc["epc_address"] == ""),
    [
        "address_1",
        "address_2",
        "postcode",
        "mcs_address",
        "mcs_postcode",
        "epc_address",
        "epc_postcode",
        "address_score",
    ],
]
mcs_epc_na

# %%
_epc_values = mcs_epc_na["epc_address"].to_list()
for val in _epc_values:
    print(len(val))

# %% [markdown]
# MCS addresses with NaN or punctuation characters only are converted to a space character (due to this line https://github.com/nestauk/asf_core_data/blob/c7886c041ed08d352a4ec6e088c1e26df90d549c/asf_core_data/pipeline/mcs/process/mcs_epc_joining.py#L51). This space character then matches with short epc_addresses (seemingly 10 or fewer characters) containing a space. This can easily be fixed in the preprocessing pipeline.

# %% [markdown]
# ## Explore errors in sample for patterns
#
# Here we will manually review addresses which have been erroneously matched. We split wrongly matched addresses into alpha-numeric addresses and alpha-only addresses and check for any visible patterns.

# %%
## Add col to identify addresses with and without numeric components
sample["numeric_address_component"] = sample.apply(
    lambda row: 0
    if (row["mcs_numeric_tokens"] == "set()") & (row["epc_numeric_tokens"] == "set()")
    else 1,
    axis=1,
)

## Get erroneous matches only
sample_error = sample[sample["confident_match"] != 1]

# %%
## See which errors have numeric address components
sample_error_num = sample_error.loc[(sample_error["numeric_address_component"] == 1)]
sample_error_num

# %% [markdown]
# From viewing the erroneous addresses with numeric components, it appears there are some patterns to the errors:
# - Address format: (house_name), number, street, postcode
# - Not enough information contained in EPC address - e.g. flat/apartment number with no building number/name or street

# %%
## See which errors don't have numeric components
sample_error_alpha = sample_error.loc[(sample_error["numeric_address_component"] == 0)]
sample_error_alpha

# %% [markdown]
# For addresses without numeric components, it appears there are some patterns to the errors:
# - Multiple buildings with the same name but different suffix - e.g. ‘example farm’ and ‘example barn’ or ‘example farm’ and ‘example farm cottage’
# - Similar spelling of house names in the same postcode

# %%
print(
    f"Number of errors in addresses in sample with numeric components: {len(sample_error_num)}"
)
print(
    f"% of errors in sample with numeric components: {round(100 * len(sample_error_num)/errors, 2)}%"
)
print(
    f"Number of errors in addresses in sample without numeric components: {len(sample_error_alpha)}"
)
print(
    f"% of errors in sample without numeric components: {round(100 * len(sample_error_alpha)/errors, 2)}%"
)

# %% [markdown]
# ## Explore address score distribution
#
# We will look at the potential for optimising the address score to see if we can reduce the errors. To do this, we will start by exploring the address score distribution.

# %%
## See distribution of address matching score in sample

sns.histplot(x="address_score", data=sample, bins=25).set_title(
    f"Address score distribution in sample data, N={len(sample)}"
)

# %%
## Compare to population address matching score distribution

_mcs_epc = mcs_epc[
    mcs_epc["address_score"] != 1
]  # remove scores of 1 because it dominates the data
sns.histplot(x="address_score", data=_mcs_epc, bins=25).set_title(
    f"Address score (below 1) distribution in population data, N={len(_mcs_epc)}"
)

# %%
## Going back to our sample, let's see what scores our errors get
sns.histplot(x="address_score", data=sample_error).set_title(
    f"Address score distribution across incorrectly matched addresses, N={len(sample_error)}"
)

# %% [markdown]
# The errors are spread across address scores and not just found in low scores. Let's see a breakdown of the rate of errors across scores.

# %%
## qcut for equal sized matched address groups

_sample = sample.copy()

_sample["address_score_qbin20"] = pd.qcut(_sample["address_score"], 20)
_match_rate = _sample.groupby("address_score_qbin20").agg({"confident_match": np.mean})

fig, ax = plt.subplots(figsize=(10, 5))

sns.barplot(x=_match_rate.index, y=_match_rate["confident_match"], ax=ax)

ax.set_title(f"Confident Match rate by address score q20 on sample, N={len(_sample)}")

plt.xticks(rotation=90)
plt.show()

# %% [markdown]
# In our sample, half of address matches with scores between 0.7-0.791 are errors, and 1/4 of address matches with scores between 0.791-0.839 are errors.

# %%
## Cut for equal sized ranges

_sample["address_score_bin15"] = pd.cut(_sample["address_score"], 15)
_match_rate = _sample.groupby("address_score_bin15").agg({"confident_match": np.mean})

fig, ax = plt.subplots(figsize=(10, 5))

sns.barplot(x=_match_rate.index, y=_match_rate["confident_match"], ax=ax)

ax.set_title(f"Confident Match rate by address score on sample, N={len(_sample)}")

plt.xticks(rotation=90)

plt.show()

# %% [markdown]
# In our sample, most address matches with address scores of 0.7-0.739 are errors. Half of address matches with address scores of 0.758-0.777 are errors. Address scores above 0.835 mostly indicate correct matches.

# %%
## Address score in numeric address errors
sns.histplot(
    x="address_score",
    data=sample_error[sample_error["numeric_address_component"] == 1],
    bins=8,
).set_title(
    f"Address score distribution in erroneous address matches for addresses with numeric components, N={len(sample_error[sample_error['numeric_address_component']==1])}"
)

# %%
## Address score in non-numeric address errors
sns.histplot(
    x="address_score",
    data=sample_error[sample_error["numeric_address_component"] == 0],
    bins=8,
).set_title(
    f"Address score distribution in erroneous address matches for addresses with no numeric components, N={len(sample_error[sample_error['numeric_address_component']==0])}"
)

# %% [markdown]
# It appears that erroneous matches of addresses with numeric components tend to have lower address scores and erroneous matches of addresses with no numeric components tend to have higher or very low address scores.

# %% [markdown]
# ## Optimise address score threshold
#
# We will assess whether we can optimise the address score threshold for improved address matching using Youden's J statistic.

# %%
rcd.from_predictions(y_true=sample["confident_match"], y_pred=sample["address_score"])

# %%
youdens_thresholds_country = {}
for country in ["England", "Scotland", "Wales"]:
    fpr, tpr, thresholds = roc_curve(
        y_true=sample[sample["country"] == country]["confident_match"],
        y_score=sample[sample["country"] == country]["address_score"],
    )
    ## Calculate Youden's J statistic
    ## sensitivity + specificity - 1 = TPR - FPR
    _df = pd.DataFrame({"youdens": tpr - fpr, "threshold": thresholds}).sort_values(
        by="youdens", ascending=False
    )
    _df["removed_candidates"] = [
        (sample["address_score"] < threshold).sum() for threshold in _df["threshold"]
    ]
    _df["removed_true_matches"] = [
        (sample[sample["confident_match"] == 1]["address_score"] < threshold).sum()
        for threshold in _df["threshold"]
    ]
    youdens_thresholds_country[country] = _df

youdens_thresholds_country

# %%
youdens_thresholds_numeric = {}
for _, key in zip([0, 1], ["non_numeric", "numeric"]):
    fpr, tpr, thresholds = roc_curve(
        y_true=sample[sample["numeric_address_component"] == _]["confident_match"],
        y_score=sample[sample["numeric_address_component"] == _]["address_score"],
    )
    ## Calculate Youden's J statistic
    ## sensitivity + specificity - 1 = TPR - FPR
    _df = pd.DataFrame({"youdens": tpr - fpr, "threshold": thresholds}).sort_values(
        by="youdens", ascending=False
    )
    _df["removed_candidates"] = [
        (sample["address_score"] < threshold).sum() for threshold in _df["threshold"]
    ]
    _df["removed_true_matches"] = [
        (sample[sample["confident_match"] == 1]["address_score"] < threshold).sum()
        for threshold in _df["threshold"]
    ]
    youdens_thresholds_numeric[key] = _df

youdens_thresholds_numeric

# %% [markdown]
# Even when we search for optimum threshold by country or by numeric/alpha address, it looks like there could be a potential optimum address score threshold between 0.84-0.869231.
#
# This optimum is based on the following assumptions:
#
# - We want to maximise Youden's J-statistic
# - i.e. sensitivity and specificity are equally important: removing false positives is as important as retaining true positives
#
# Based on these results and the confident match rate by address scores assessed above, we should consider increasing the address score threshold at least to 0.74 and potentially to somewhere between 0.77 and 0.87.
#
# However, we lose true positive matches with every increase in the threshold. Also, simply increasing the threshold cannot remove many of the erroneous matches in non-numeric addresses because many of these have very high similarity scores. We will need a different methodology to address these errors. We will explore alternative improvements to address matching in a separate analysis.
