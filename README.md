# ASF Core Data

Last updated: March 11 2022 by Julia Suter

## Overview

The ASF Core Data repository provides a codebase for **retrieving, cleaning, processing and combining** datasets that are of great relevance to the Sustainable Future Mission at Nesta.

A large section of this repository is dedicated to the **Energy Performance Certificate (EPC) Register**. Another part helps validating, cleaning and processing a subset of the **Microgeneration Certification Scheme (MCS)** database, includings data about heat pump installations and heat pump installer companies.

In addition, the repository includes various other datasets that are of potential use:

- Index of Multiple Deprivation
- UK Postcodes to coordinates
- [more to be added]

# Setup and Contributions

Feel free to use this repository as a basis for your own work, raise issues, offer pull requests or reach out to us with questions and feedback.

### Setup

- Meet the data science cookiecutter [requirements](http://nestauk.github.io/ds-cookiecutter/quickstart), in brief:
  - Install: `git-crypt`, `direnv`, and `conda`
  - Have a Nesta AWS account configured with `awscli`
- Run `make install` to configure the development environment:
  - Setup the conda environment
  - Configure pre-commit
  - Configure metaflow to use AWS

### Contributor guidelines

[Technical and working style guidelines](https://github.com/nestauk/ds-cookiecutter/blob/master/GUIDELINES.md)

---

<small><p>Project based on <a target="_blank" href="https://github.com/nestauk/ds-cookiecutter">Nesta's data science project template</a>
(<a href="http://nestauk.github.io/ds-cookiecutter">Read the docs here</a>).
</small>

### Access Restrictions

The EPC Register and the Index of Multiple Deprivation are open-source datasets accessible to everyone.

We do not own the MCS dataset so we cannot share it. Access will only be granted to Nesta employees. If you require access to the MCS dataset, please reach out to [add email address].

The Cleansed EPC dataset was provided by EST and we are not allow to share that version. For access, please reach out to [add email address].

### Raw Data

In `inputs/EPC_data/raw_data` you can find the raw EPC data. The current version holds the data up to the second quarter of 2021.

The data for England and Wales can be found in `inputs/EPC_data/raw_data/england_wales` in a zipped file named _all-domestic-certificates.zip_.

The data for Scotland can be found in `inputs/EPC_data/raw_data/scotland` in a zipped file named _D_EPC_data.csv_.

| Selection (Q4_2021) | Samples    |
| ------------------- | ---------- |
| GB                  | 22 840 162 |
| England             | 20 318 501 |
| Wales               | 1 111 383  |
| Scotland            | 1 410 278  |

### Cleansed EPC (EST)

**IMPORTANT: We are not allowed to share this dataset without explicit permission by EST.**

In `inputs/EPC_data/EST_cleansed_versions` you can find a cleansed version of the EPC data for England and Wales provided by EST.

EST selected a set of relevant features, cleaned them and got rid of erroneous values. They also identified duplicates. Both versions with and without deduplication are available as zipped files:

- EPC_England_Wales_cleansed.csv.zip
- EPC_England_Wales_cleansed_and_deduplicated.csv.zip (14.4 million samples)

This version does not include the most recent EPC data as it only contains entries until the first quarter of 2020. It also does not include any data on Scotland.

The cleansed set includs the following 45 features:

```
ROW_NUM
LMK_KEY
ADDRESS1
ADDRESS2
ADDRESS3
POSTCODE
BUILDING_REFERENCE_NUMBER
LOCAL_AUTHORITY
LOCAL_AUTHORITY_LABEL
CONSTITUENCY
COUNTY
LODGEMENT_DATE
FINAL_PROPERTY_TYPE
FINAL_PROP_TENURE
FINAL_PROPERTY_AGE
FINAL_HAB_ROOMS
FINAL_FLOOR_AREA
FINAL_WALL_TYPE
FINAL_WALL_INS
FINAL_RIR
FINAL_LOFT_INS
FINAL_ROOF_TYPE
FINAL_MAIN_FUEL
FINAL_SEC_SYSTEM
FINAL_SEC_FUEL_TYPE
FINAL_GLAZ_TYPE
FINAL_ENERGY_CONSUMPTION
FINAL_EPC_BAND
FINAL_EPC_SCORE
FINAL_CO2_EMISSIONS
FINAL_FUEL_BILL
FINAL_METER_TYPE
FINAL_FLOOR_TYPE
FINAL_FLOOR_INS
FINAL_HEAT_CONTROL
FINAL_LOW_ENERGY_LIGHTING
FINAL_FIREPLACES
FINAL_WIND_FLAG
FINAL_PV_FLAG
FINAL_SOLAR_THERMAL_FLAG
FINAL_MAIN_FUEL_NEW
FINAL_HEATING_SYSTEM
```

### Preprocessed EPC Dataset

In `inputs/EPC_data/preprocessed_data/Q4_2021` you can find three different versions of preprocessed EPC data.

**Since the preprocessing depends on data cleaning and feature engineering algorithms that may change over time, the data in this folder should be considered a snapshot of the current status in September 2021. Ideally, you should always work with the output of the most recent preprocessing version.**

You can generate the preprocessed datasets from the raw data by executing the script in _preprocess_epc_data.py_ in `asf_core_data/pipeline/preprocessing`.

It will generate three versions of the data in `outputs/EPC_data/preprocessed_data/Q[quarter]_[YEAR]`. They will be written out as regular CSV-files.

| Filename                                 | Version                                         | Samples    |
| ---------------------------------------- | ----------------------------------------------- | ---------- |
| EPC_GB_raw.csv                           | Original raw data                               | 22 840 162 |
| EPC_GB_preprocessed.csv                  | Cleaned and added features, includes duplicates | 22 839 568 |
| EPC_GB_preprocessed_and_deduplicated.csv | Cleaneda and added features, without duplicates | 18 179 719 |

_EPC_GB_raw.csv_ merges the data for England, Wales and Scotland in one file, yet leaves the data unaltered.

The preprocessing steps include cleaning and standardising the data and adding additional features. For a detailed description of the proprocessing consult this documentation [work in progress].

We also identified duplicates, i.e. samples referring to the same property yet often at different points in times. A preprocessed version of the EPC data without duplicates can be found in _EPC_GB_preprocessed_and_deduplicated.csv_.

Since duplicates can be interesting for some research questions, we also save the version with duplicates included as _EPC_GB_preprocessed.csv_.

**Note**: In order make up/downloading more efficient, we zipped the files in `inputs/EPC_data/preprocessed_data` - so the filenames all end in _.zip_. The data loading script in `asf_core_data/pipeline/getters/epc_data.py` will handle the unzipping if necessary.

## EPC Dataset

Last updated: 22 October 2021 by Julia Suter

**This documentation describes the different EPC data versions for England, Wales and Scotland and how the the data was preprocessed in order to facilitate data analysis.**

### Original EPC Data

##### Download

The original data was for England and Wales is freely available [here](https://epc.opendatacommunities.org/). The data for Scotland can be downloaded from [here](https://statistics.gov.scot/resource?uri=http%3A%2F%2Fstatistics.gov.scot%2Fdata%2Fdomestic-energy-performance-certificates). You may need to sign in with your email address.

Note that the data is updated every quarter so in order to work with the most recent data, follow the steps under Data Updates.

| Version                   | # Samples  | # Features |
| ------------------------- | ---------- | ---------- |
| Original raw data         | 22 840 162 | 40         |
| After cleaning            | 22 840 162 | 40         |
| After adding features     | 22 840 162 | 54         |
| After removing duplicates | 181 797 19 | 54         |

##### Data Updates

The EPC registry is updated 4 times a year, so every quarter. In order to work with the most recent data, the EPC data needs to be downloaded and preprocessed.

**Current version: Q4_2021**

Below we describe the necessary steps to update the data. It's actually less complicated than it looks:

- Download most recent England/Wales data from [here](https://epc.opendatacommunities.org/https://epc.opendatacommunities.org/). The filename is _all-domestic-certificates.zip_.

- Download most recent Scotland data from [here](https://statistics.gov.scot/resource?uri=http%3A%2F%2Fstatistics.gov.scot%2Fdata%2Fdomestic-energy-performance-certificates). The filename is is of the format `D_EPC_data_2012-[year]Q[quarter]_extract_[month][year].zip`, for example `D_EPC_data_2012-2021Q4_extract_0721.zip`.

- Clear the folders `inputs/EPC_data/raw_data/England_Wales` and `inputs/EPC_data/raw_data/Scotland`. If there is raw EPC data from previous quarters/years, delete it or move it to a subfolder. Note that the most recently downlaoded data will include all the data from previous downloads so there should be no data loss when overwriting previous data.

- Move the downloaded zip files to the corresponding folders `England_Wales` or `Scotland`.

- Shorten the Scotland filename to `D_EPC_data.zip`.

- Note: The zipped Scotland data may not be processable by the Python package _ZipFile_ because of an unsupported compression method. This problem can be solved easily solved by unzipping and zipping the data manually, e.g. with the command `unzip` Make sure the filename remains `D_EPC_data.zip`.

- Create a new folder in `outpus/EPC_data/preprocessed_data/` with the name pattern `Q[quarter]_[year]`, indicating when the data was updated last. This information will be displayed when downloading the data and is reflected in the original filename for Scotland. For example, _Q4_2021_ includes the data up to June 2021.

- Open the config file `config/base.yaml` and update all paths including `outputs/EPC_data/preprocessed_data/Q[quarter]_[year]` to match the folder created in the last step.

- Run the preprocessing script`python -m asf_core_data.pipeline.preprocessing.preprocess_epc_data` which generates the preprocessed data in folder `outputs/EPC_data/preprocessed_data/Q[quarter]_[year]`

  Note: The data preprocessing is optimised for the data collected in 2021 (Q4_2021). More recent EPC data may include values not yet covered by the current preprocessing algorithm (for example new construction age bands), possibly causing errors when excuting the script.
  These can usually be fixed easily so feel free to open an issue or submit a pull request.

- If you update the `/inputs` data on the S3 bucket, please let everyone know by creating an issue.

- Enjoy your updated EPC data.

### Removing Duplicates

### Data Cleaning

In this section, we describe how we cleaned/standardised the EPC dataset's existing features.

#### POSTCODE

We make sure all postcodes are represented with uppercase letters. There is also the option of removing the white space between the two sections of the postcode, although it is not applied by default.

#### Dates

The date features INSPECTION_DATE and LODGEMENT_DATE need to be reformatted into `year/month/day` because the England/Wales and Scotland data do not use the same delimiter and order.

#### TENURE

The tenure type or sectors are standardised into 4 categories:

- rental (social)
- rental (private)
- owner-occupied
- unknown

The Scotland data uses _rented_ instead of _rental_, which is fixed at this point. Values such as `NO DATA!`, `INVALID`, `not defined` or `NaN` are all mapped to _unknown_.

#### CONSTRUCTION_AGE_BAND

Unfortunately, the England/Wales and Scotland data use different age bands. For instance, Scotland has a age band ranging from 1984-1991 while England/Wales has 1983-1990.

We standardise this two ways. The first one keeps the original age bands, yet labels them by country.

We mapped all values such as `NO DATA!`, `INVALID`, `not defined` or `NaN` to _unknown_. For some buildings, the value is a single year, which is mapped to its repsective age band, e.g. 2015 is mapped to _England and Wales 2012 onwards_. We only find such examples for England/Wales.

- England and Wales: before 1900
- England and Wales: 1900-1929
- England and Wales: 1930-1949
- England and Wales: 1950-1966
- England and Wales: 1967-1975
- England and Wales: 1976-1982
- England and Wales: 1983-1990
- England and Wales: 1991-1995
- England and Wales: 1996-2002
- England and Wales: 2003-2006
- England and Wales: 2007 onwards
- England and Wales: 2012 onwards
- Scotland: before 1919
- Scotland: 1919-1929
- Scotland: 1930-1949
- Scotland: 1950-1964
- Scotland: 1965-1975
- Scotland: 1976-1983
- Scotland: 1984-1991
- Scotland: 1992-1998
- Scotland: 1999-2002
- Scotland: 2003-2007
- Scotland: 2008 onwards
- unknown

In a second step, we merge the age bands for England/Wales and Scotland into (slightly overlapping) new ranges. This very helpful when trying to compare construction age across countries and generally for having a standardised way of representing construction age.

- England and Wales: before 1900
- Scotland: before 1919
- 1900-1929
- 1930-1949
- 1950-1966
- 1965-1975
- 1976-1983
- 1983-1991
- 1991-1998
- 1996-2002
- 2003-2007
- 2007 onwards
