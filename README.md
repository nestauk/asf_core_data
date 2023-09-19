# ASF Core Data <a name="core_data_overview"></a>

Last updated: September 2023 by Sofia Pinto

## Overview <a name="overview"></a>

The ASF Core Data repository provides a codebase for **retrieving, cleaning, processing and combining datasets** that are of great relevance to the [sustainable future mission at Nesta](https://www.nesta.org.uk/sustainable-future/). The main goal of our mission is to accelerate home decarbonisation, for instance by increasing the uptake of heat pumps. Read more about A Sustainable Future at Nesta [here](https://www.nesta.org.uk/sustainable-future/).

<img src="./documentation/Home-PNG-HD.png" alt="drawing" style="width:200px;"/>

This repo contains scripts to validate, clean and process:

- **Energy Performance Certificate (EPC) Register** data;
- A subset of the **Microgeneration Certification Scheme (MCS)** Installations Database (MID), including data about heat pump installations and heat pump installation companies.

Both datasets will be updated with new data on a regular basis (usually every 3 months). Data is saved in Nesta's S3, in the `asf-core-data` bucket. Check [here](#recent_data) for the most recent updates.

In addition, our S3 bucket (and repo) includes various other datasets that are of potential use:

- Index of Multiple Deprivation
- UK Postcodes to coordinates

This repository is the foundation for many projects in our work on sustainability at Nesta. [Explore](#projects) the variety of projects that is based on these datasets.

## Table of Contents <a name="top"></a>

- [Most Recent Data](#recent_data)
- [Setup and Contributions](#setup)
- [Datasets in Detail](#datasets)
  - [Access Restrictions](#access)
  - [EPC](#epc)
  - [MCS](#mcs)
- [Using this repo](#using_this_repo)
  - [Structure](#structure)
  - [Getting EPC/MCS data for your projects](#data_for_projects)
  - [Processing new data](#processing_new_data)
- [Project Examples](#projects)

## Most Recent Data Updates <a name="recent_data"></a>

Generally, both the EPC and MCS data will be updated every three months. Release dates can vary, as we are not responsible for the release of the raw data.

| Dataset                     | Content       | Last updated    | Includes data up to |
| --------------------------- | ------------- | --------------- | ------------------- |
| EPC England/Wales           | up to Q1 2023 | 25 May 2023     | 30 April 2023       |
| EPC Scotland                | up to Q1 2023 | 13 June 2023    | 30 April 2023       |
| MCS Heat Pump Installations | up to Q1 2023 | 19 April 2023   | 31 March 2023       |
| MCS HP Installer Data       | up to Q4 2022 | 7 February 2023 | January 2023        |

<a href="#top">[back to top]</a>

## Setup and Contributions <a name="setup"></a>

### Setup

- Meet the data science cookiecutter [requirements](http://nestauk.github.io/ds-cookiecutter/quickstart), in brief:
  - Install: `git-crypt`, `direnv`, and `conda`
  - Have a Nesta AWS account configured with `awscli`
- Run `make install` to configure the development environment:
  - Setup the conda environment
  - Configure pre-commit
  - Configure metaflow to use AWS

### Contributor guidelines

Everyone is welcome to contribute to our work: raise issues, offer pull requests or reach out to us with questions and feedback. Feel free to use this repository as a basis for your own work - but it would be great if you could cite or link to this repository and our work.

[Technical and working style guidelines](https://github.com/nestauk/ds-cookiecutter/blob/master/GUIDELINES.md)

<p>Project based on <a target="_blank" href="https://github.com/nestauk/ds-cookiecutter">Nesta's data science project template</a> (<a href="http://nestauk.github.io/ds-cookiecutter">Read the docs here</a>).

<a href="#top">[back to top]</a>

# Datasets <a name="datasets"></a>

## Data Access Restrictions <a name="access"></a>

The **EPC Register** and the **Index of Multiple Deprivation** are open-source datasets accessible to everyone. The **Cleansed EPC dataset** was kindly provided by the Energy Saving Trust (EST) and we are not allowed to share that version of the EPC data. For access, please reach out to EST directly or ask us for their contact details.

The **MID (MCS Installations Database)** belongs to MCS, including the subset of data we work on. Thus, we cannot share the raw data and access will only be granted to Nesta employees. If you require access to this data, please reach out to MCS directly or ask us for their contact details.

## EPC Register <a name="epc"></a>

The EPC Register provides data on building characteristics and energy efficiency, including:

- Address/location information;
- Chacteristics such as number of rooms;
- Energy efficiency ratings;
- Heating system(s).

Visit the respective websites for [England and Wales](https://epc.opendatacommunities.org/) and [Scotland](https://statistics.gov.scot/resource?uri=http%3A%2F%2Fstatistics.gov.scot%2Fdata%2Fdomestic-energy-performance-certificates) to learn more. [Here](https://epc.opendatacommunities.org/docs/guidance) you can also find guidance (such as coverage & background) and metadata for England and Wales EPC data. In our mission, we only work with domestic EPC data, as we are focused on home decarbonisation.

Raw EPC data can be found in `asf_core_data` S3 bucket under the following path: `inputs/data/EPC/raw_data/`. In the `raw_data` folder you will find multiple folders corresponding to different batches of EPC data. As an example, folder`/2023_Q1_complete/` (and corresponding `.zip` file, `2023_Q1_complete.zip`) contains all raw EPC data up to Q1 of 2023.

### Cleansed EPC (EST) <a name="cleansed_epc"></a>

EST selected a set of relevant features, cleaned them and got rid of erroneous values. They also identified duplicates. You'll find a cleansed version of the EPC data for England and Wales provided by EST on `asf-core-data` S3 bucket, under `/inputs/EPC_data/EST_cleansed_versions/`.

Both versions with and without deduplication are available as zipped files:

- `EPC_England_Wales_cleansed.csv.zip`
- `EPC_England_Wales_cleansed_and_deduplicated.csv.zip`

This version does not include the most recent EPC data as it only contains entries until the first quarter of 2020. It also does not include any data on Scotland.

The cleansed set includs the following 45 features:

```
ROW_NUM, LMK_KEY, ADDRESS1, ADDRESS2, ADDRESS3, POSTCODE, BUILDING_REFERENCE_NUMBER, LOCAL_AUTHORITY, LOCAL_AUTHORITY_LABEL, CONSTITUENCY, COUNTY, LODGEMENT_DATE, FINAL_PROPERTY_TYPE, FINAL_PROP_TENURE, FINAL_PROPERTY_AGE, FINAL_HAB_ROOMS, FINAL_FLOOR_AREA, FINAL_WALL_TYPE, FINAL_WALL_INS, FINAL_RIR, FINAL_LOFT_INS, FINAL_ROOF_TYPE, FINAL_MAIN_FUEL, FINAL_SEC_SYSTEM, FINAL_SEC_FUEL_TYPE, FINAL_GLAZ_TYPE, FINAL_ENERGY_CONSUMPTION, FINAL_EPC_BAND, FINAL_EPC_SCORE, FINAL_CO2_EMISSIONS, FINAL_FUEL_BILL, FINAL_METER_TYPE, FINAL_FLOOR_TYPE, FINAL_FLOOR_INS, FINAL_HEAT_CONTROL, FINAL_LOW_ENERGY_LIGHTING, FINAL_FIREPLACES, FINAL_WIND_FLAG, FINAL_PV_FLAG, FINAL_SOLAR_THERMAL_FLAG, FINAL_MAIN_FUEL_NEW, FINAL_HEATING_SYSTEM
```

**IMPORTANT: We are not allowed to share this dataset without explicit permission by EST.**

<a href="#epc">[back to top of EPC]</a>

### Preprocessed EPC Dataset <a name="preprocessed_epc"></a>

We've developed our own processing pipeline for domestic EPC data from England, Wales and Scotland. This allows us to get the most up-to-date batch of data every quarter and run our preprocessing pipeline which cleans, deduplicates, processes and joins raw EPC domestic data from England, Wales and Scotland.

**asf_core_data/pipeline/preprocessing** contains functions for processing the raw EPC data. In particular, functions in `pipeline/preprocessing/preprocess_epc_data.py` will process and save three different versions of the data to S3. Preprocessed data can be found in `asf-core-data` S3 bucket, following the path: `/outputs/EPC/preprocessed_data/`. In the `preprocessed_data` folder, you you can find three different versions of preprocessed EPC data for quarter `quarter` and year `YEAR`, e.g. `/YEAR_Qquarter_complete/`. For example `/2023_Q1_complete/` contains preprocessed versions of the data, up to Q1 of 2023.

Since the preprocessing depends on data cleaning and feature engineering algorithms that may change over time, ideally, you should always work with the output of the most recent preprocessing version.

Inside each quarter folder, you will find different versions of the data (both as `.csv` and as `.zip`):

- `EPC_GB_raw`: contains all raw data from England, Wales and Scotland in the same file; at this point, no processing has been done, only column names have been changed;
- `EPC_preprocessed`: contains preprocessed/enhanced data (new columns, changes to values, masked values, etc);
- `EPC_preprocessed_and_deduplicated`: contains preprocessed data with only the most up-to-date record for each house; hence, this is not a true "deduplication" as multiple EPC records for the same house, will be different (at least the date of inspection will be different).

## MCS Data <a name="mcs"></a>

The MCS datasets contain information on MCS-certified heat pump **installations** and **installation companies** (or **installers**). These datasets are proprietary and cannot be shared outside of Nesta. Both datasets are provided quarterly by MCS and are stored in Nesta's **asf-core-data** bucket on S3. Information about the fields in the raw dataset can be found [here](https://docs.google.com/spreadsheets/d/1XaGDblbCIBTkStH3_RE7d6qzzKAWghRf/edit#gid=1260528248).

### Installations <a name="mcs_installations"></a>

The MCS **installations** dataset contains one record for each MCS certificate. In almost all cases, each MCS certificate corresponds to a single heat pump system installation (which in some cases may feature multiple heat pumps). The dataset contains records of both domestic and non-domestic installations. Features in the dataset include

- characteristics of the property: address, heat and water demand, alternative heating system
- characteristics of the installed heat pump: model, manufacturer, capacity, flow temperature, SCOP
- characteristics of the installation: commissioning date, overall cost, installer name, whether or not the installation is eligible for RHI
- characteristics of the certificate: version number, certification date

For further information about the data collection process see [this doc](https://docs.google.com/document/d/1uuptYecUfIm1Dxy1iuw19xgareZPzg_WP4M7J80mTgc/edit?usp=sharing) (assessible only to Nesta employees).

Raw MCS installations data is stored in `asf-core-data` S3 bucket, following the path `/inputs/MCS/latest_raw_data/historical/`, e.g. `raw_historical_mcs_installations_YYYYMMDD.xlsx` where `DD/MM/YYYY` is the date the data was shared.

### Installers <a name="mcs_installers"></a>

The MCS **installers** dataset contains one record for each MCS-certified installer. Features in the dataset include

- name and address
- technologies installed

### Procesing MCS and merging with EPC <a name="mcs_epc_merging"></a>

**asf_core_data/pipeline/mcs** contains functions for processing the raw MCS data and joining it with EPC data. In particular, running **generate_mcs_data.py** will process and save four different datasets to S3 (to `/outputs/MCS/`):

1. Cleaned MCS installation data, with one row for each **installation**, e.g. `mcs_installations_220713.csv`
2. As in 1., with EPC data fully joined: when a property also appears in EPC data, a row is included for each EPC (so the full EPC history of the property appears in the data), e.g. `mcs_installations_epc_full_220713.csv`
3. As in 2., filtered to the "most relevant" EPC which aims to best reflect the status of the property at the time of the installation: the latest EPC before the installation if one exists, otherwise the earliest EPC after the installation, e.g. `mcs_installations_epc_most_relevant_220713.csv`
4. Preprocessed MCS installation companies/installers data, e.g. `installers/mcs_historical_installers_20230207.csv`

Further details about the processing and joining of the data are provided within `asf_core_data/pipeline`.

<a href="#top">[back to top]</a>

</mark>

# Using this repo <a name="using_this_repo"></a>

## Dir Structure <a name="structure"></a>

<mark>to be added</mark>

<a href="#top">[back to top]</a>

## Getting EPC/MCS data for your projects <a name="data_for_projects"></a>

To pull installation data or energy certificates data into a project, first install this repo as a package by adding this line to your project's `requirements.txt`, replacinh `$BRANCH` for your desired branch name (tipically, you should use the `dev` branch, which has been reviewed and merged):

    asf_core_data@ git+ssh://git@github.com/nestauk/asf_core_data.git@$BRANCH

### Getting EPC data

EPC data can be pulled directly from S3 or downloaded and then loaded from a local folder. Because EPC data is very big, we tipically download the data and load it using getter functions.

You can follow [this jupyter notebook](https://github.com/nestauk/asf_core_data/blob/dev/asf_core_data/analysis/data_loading.ipynb) on how to download and load the multiple versions of EPC data.

As an example, you can load a couple of variables of the most up-to-date batch of preprocessed and deduplicated EPC data:

```
from asf_core_data.getters import data_getters
from asf_core_data import load_preprocessed_epc_data

LOCAL_DATA_DIR = 'path/to/data/dir' #e.g. '.../Documents/ASF data/'

data_getters.download_core_data('epc_preprocessed_dedupl', LOCAL_DATA_DIR, batch="newest")

prep_epc_dedupl = load_preprocessed_epc_data(data_path=LOCAL_DATA_DIR, version='preprocessed_dedupl', usecols=['CURRENT_ENERGY_EFFICIENCY', 'PROPERTY_TYPE', 'BUILT_FORM', 'CONSTRUCTION_AGE_BAND', 'COUNTRY'])
```

### Getting MCS data

MCS heat pump installations data can be pulled in using:

    from asf_core_data import get_mcs_installations

    installation_data = get_mcs_installations(epc_version=...)

Replace the `...` above with

- "none" to get just the MCS installation records
- "full" to get every MCS installation record joined to the property's full EPC history (one row for each installation-EPC pair)
  - installations without a matching EPC are kept, but with missing data in the EPC fields;
  - also note that multiple installations might be wrongly associated to the same EPC record (due to missing house number in address data, for example);
- "most_relevant" to get every MCS installation joined to its "most relevant" EPC record (this being the latest EPC before the installation if one exists; otherwise, the earliest EPC after the installation) - installations without a matching EPC are kept, but with missing data in the EPC fields

## Processing new data <a name="processing_new_data"></a>

### EPC data

To preprocess EPC data, run:
`python -m asf_core_data/pipeline/preprocessing/preprocess_epc_data.py`. This will get raw data from the S3 bucket and process it.

Alternatively, you can run `python asf_core_data/pipeline/preprocessing/preprocess_epc_data.py --path_to_data "/LOCAL/PATH/TO/DATA/"` if you want to read raw data from a local path.

Thi will generate three versions of the data in `outputs/EPC_data/preprocessed_data/Q[quarter]_[YEAR]`. They will be written out as regular CSV-files.

| Filename                                 | Version                                         | Samples (Q4_2021) |
| ---------------------------------------- | ----------------------------------------------- | ----------------- |
| EPC_GB_raw.csv                           | Original raw data                               | 22 840 162        |
| EPC_GB_preprocessed.csv                  | Cleaned and added features, includes duplicates | 22 839 568        |
| EPC_GB_preprocessed_and_deduplicated.csv | Cleaneda and added features, without duplicates | 18 179 719        |

_EPC_GB_raw.csv_ merges the data for England, Wales and Scotland in one file, yet leaves the data unaltered.

The preprocessing steps include cleaning and standardising the data and adding additional features. For a detailed description of the proprocessing consult this documentation [work in progress].

We also identified duplicates, i.e. samples referring to the same property yet often at different points in times. A preprocessed version of the EPC data without duplicates can be found in _EPC_GB_preprocessed_and_deduplicated.csv_.

Since duplicates can be interesting for some research questions, we also save the version with duplicates included as _EPC_GB_preprocessed.csv_.

**Note**: In order make up- and downloading more efficient, we zipped the files in `outputs/EPC_data/preprocessed_data` - so the filenames all end in _.zip_.

<a href="#epc">[back to top of EPC]</a>

### Downloading and Updating EPC Data <a name="epc_update"></a>

The original data for England and Wales is freely available [here](https://epc.opendatacommunities.org/). The data for Scotland can be downloaded from [here](https://statistics.gov.scot/resource?uri=http%3A%2F%2Fstatistics.gov.scot%2Fdata%2Fdomestic-energy-performance-certificates). You may need to sign in with your email address.

Note that the data is updated every quarter so in order to work with the most recent data, the EPC data needs to be downloaded and preprocessed. [Here](#recent_data) you can check when the data was updated last time. If you need to download or update the EPC data, follow the steps below.

Below we describe the necessary steps to download and update the data:

- Create a local folder in your computer for ASF core data (if you don't have one already), e.g. `/Documents/ASF data/`. Inside create the following path `/inputs/EPC/raw_data/`

- Download most recent England/Wales data from [here](https://epc.opendatacommunities.org/https://epc.opendatacommunities.org/). The filename is _all-domestic-certificates.zip_.

- Download most recent Scotland data from [here](https://statistics.gov.scot/resource?uri=http%3A%2F%2Fstatistics.gov.scot%2Fdata%2Fdomestic-energy-performance-certificates). The filename is is of the format `D_EPC_data_2012-[year]Q[quarter]_extract_[month][year].zip`, for example `D_EPC_data_2012-2021Q4_extract_0721.zip`.

- Create a folder for the specific quarter data inside `inputs/data/EPC/raw_data/`, following the structure `inputs/data/EPC/raw_data/YYYY_Qm_complete/` e.g.`inputs/data/EPC/raw_data/2022_Q4_complete/`

- Inside your quarter folder, create a subfolder called `England_Wales` and move the downloaded zip England and Wales file to the folder. Create a subfolder called `Scotland` and move the Scotland zip file to the subfolder.

- Shorten the Scotland filename to `D_EPC_data.zip`.

- Note: The zipped Scotland data may not be processable by the Python package _ZipFile_ because of an unsupported compression method. This problem can be solved easily solved by unzipping and zipping the data manually, e.g. with the command `unzip`. Make sure the filename remains `D_EPC_data.zip`.

- Upload raw data to S3 `asf-core-data/inputs/EPC/raw_data/` (as well as a zipped file). You can do that by:

  - `aws s3 cp SOURCE_DIR s3://asf-core-data/inputs/EPC/raw_data/ --recursive`, where `SOURCE_DIR` is the your local path to the raw EPC batch data
  - Alternatively, you can log-in to S3 in your browser, and do the upload directly there.

- Run `python asf_core_data/pipeline/preprocessing/preprocess_epc_data.py --path_to_data "/LOCAL/PATH/TO/DATA/"` which generates the preprocessed data in folder `outputs/EPC/preprocessed_data/[YEAR]_Q[quarter]_complete`, e.g. `outputs/EPC/preprocessed_data/2023_Q1_complete`

  Note: The data preprocessing is optimised for the data collected in 2023 (Q1_2023). More recent EPC data may include values not yet covered by the current preprocessing algorithm (for example new construction age bands), possibly causing errors when excuting the script.
  These can usually be fixed easily so feel free to open an issue or submit a pull request.

- Zip the output files and upload them, as well as the .csv files to S3:

  - using the `aws s3 cp` command again as above;
  - Alternatively, you can log-in to S3 in your browser, and do the upload directly there.

- If you update the `/inputs` data on the S3 bucket, please let everyone in the ASF data science team know and update this `README.md` file.

<a href="#epc">[back to top of EPC]</a>

<a href="#top">[back to top]</a>

## MCS data (and merges to EPC)

To update installations and installer data on the `asf-core-data` S3 bucket, run:

    export COMPANIES_HOUSE_API_KEY="ADD_YOUR_API_KEY_HERE"

    python asf_core_data/pipeline/mcs/generate_mcs_data.py

which requires you to have a Companies House API key:

- Create a developer account for Companies House API [here](https://developer.company-information.service.gov.uk/get-started);
- Create a new application [here](https://developer.company-information.service.gov.uk/manage-applications/add). Give it any name and description you want and choose the Live environment. Copy your API key credentials.

Note that the code above will save data directly to S3 and it doesn't require data to be in a local folder (i.e. it reads data directly from S3).

To run checks on raw installations data:

    from asf_core_data import test_installation_data

    test_installation_data(filename)

# Project Examples <a name="projects"></a>

<mark>More to follow and to be polished, just some examples</mark>

### Predicting the next wave of heat pump adopters

GitHub: https://github.com/nestauk/heat_pump_adoption_modelling
Report: [follows]

As part of our mission to accelerate household decarbonisation by increasing the uptake of low-carbon heating, we recently embarked on a project in partnership with the Energy Saving Trust centred around using machine learning and statistical methods to gain insights into the adoption of heat pumps across the UK.

**Supervised learning**
This approach uses supervised machine learning to predict heat pump adoption based on the characteristics of current heat pump adopters and their properties. First, we train a model on historical data to predict whether an individual property had installed a heat pump by 2021, which reveals what factors are most informative in predicting uptake. An alternative model takes the slightly broader approach of predicting the growth in heat pump installations at a postcode level instead of individual households, indicating which areas are more likely to adopt heat pumps in the future.

**Geostatistical modelling**
This approach uses a geostatistical framework to model heat pump uptake on a postcode level. First, we aggregate the household EPC data to summarise the characteristics of the properties in each postcode (for instance, their average floor area). We then model the number of heat pumps in a postcode based on the characteristics of its properties and on the level of adoption in nearby postcodes. This allows us to uncover patterns specifically related to the spatial distribution of heat pump adoption, which can be represented on maps in an accessible way.

### Low Carbon Maps

GitHub: https://athrado.github.io/data_viz/
Report: follows

Browse through the variety of maps, showing the energy efficiency ratings for various tenure sectors to the adoption of heat pumps.

<a href="#projects">[back to top of Project Examples]</a>

<a href="#top">[back to top]</a>
