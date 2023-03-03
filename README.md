# ASF Core Data <a name="core_data_overview"></a>

Last updated: July 12 2022 by Chris Williamson

## Overview <a name="overview"></a>

The ASF Core Data repository provides a codebase for **retrieving, cleaning, processing and combining datasets** that are of great relevance to the Sustainable Future Mission at Nesta. Our goal is to accelerate home decarbonisation, for instance by increasing the uptake of heat pumps. Read more about A Sustainable Future at Nesta [here](https://www.nesta.org.uk/sustainable-future/).

<img src="./documentation/Home-PNG-HD.png" alt="drawing" style="width:200px;"/>

A large section of this repository is dedicated to the **Energy Performance Certificate (EPC) Register**. Another section helps validating, cleaning and processing a subset of the **Microgeneration Certification Scheme (MCS)** Installations Database (MID), including data about heat pump installations and heat pump installer companies.

Both datasets will be updated with new data on a regular basis. Check [here](#recent_data) for the most recent updates.

In addition, the repository includes various other datasets that are of potential use:

- Index of Multiple Deprivation
- UK Postcodes to coordinates
- <mark>[more to be added] </mark>

This repository is the foundation for many projects in our work on sustainability at Nesta. [Explore](#projects) the variety of projects that is based on these datasets.

## Table of Content <a name="top"></a>

[ASF Core Data - Overview](#core_data_overview)

- [Overview](#overview)
- [Table of Content](#toc)
- [Setup and Contributions](#setup)
- [Access Restrictions](#access)
- [Most Recent Data](#recent_data)

[Datasets in Detail](#datasets)

- [Structure](#structure)
- [EPC](#epc)
  - [Cleansed EPC](#cleaned_epc)
  - [Preprocessed EPC](#preprocessed_epc)
  - [Downloading and Updating](#epc_update)
- [MCS](#mcs)
  - [Installations](#mcs_installations)
  - [Installers](#mcs_installers)
  - [Merging with EPC](#mcs_epc_merging)

[Project Examples](#projects)

## Most Recent Data Updates <a name="recent_data"></a>

Generally, both the EPC and MCS data will be updated every three months. Release dates can vary as we are not responsible for the release of the raw data.

| Dataset                     | Content       | Last updated        |
| --------------------------- | ------------- | ------------------- |
| EPC England/Wales           | up to Q4 2021 | March 14th, 2022    |
| EPC Scotland                | up to Q2 2021 | March 14th, 2022    |
| MCS Heat Pump Installations | up to Q4 2021 | February 25th, 2022 |
| MCS HP Installer Data       | up to Q4 2021 | March 15th, 2022    |

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

## Data Access Restrictions <a name="access"></a>

The EPC Register and the Index of Multiple Deprivation are open-source datasets accessible to everyone.

The MID (MCS Installations Database) belongs to MCS, as well as the subset we work on. Thus, we cannot share the raw data and access will only be granted to Nesta employees. If you require access to this data, please reach out to MCS directly or ask us for contact details.

The Cleansed EPC dataset was kindly provided by EST and we are not allowed to share that version. For access, please reach out to EST directly or ask us for contact details.

# Datasets <a name="datasets"></a>

## Dir Structure <a name="structure"></a>

<mark>to be added</mark>

<a href="#top">[back to top]</a>

## EPC Register <a name="epc"></a>

The EPC Register provides data on building characteristics and energy efficiency. Visit the respective websites for [England and Wales](https://epc.opendatacommunities.org/) and [Scotland](https://statistics.gov.scot/resource?uri=http%3A%2F%2Fstatistics.gov.scot%2Fdata%2Fdomestic-energy-performance-certificates) to learn more.

You can retrieve the data either by downloading it or pulling it from the S3 bucket with `make inputs-pull`. If you choose to download the data, please follow the steps [here](#epc_update).

In `inputs/data/EPC/raw_data` you can find the raw EPC data. The current version holds up to 2021.

The data for England and Wales can be found in `inputs/data/EPC/raw_data/england_wales` in a zipped file named _all-domestic-certificates.zip_.

The data for Scotland can be found in `inputs/data/EPC/raw_data/scotland` in a zipped file named _D_EPC_data.csv_.

| Selection (Q4_2021) | Samples    |
| ------------------- | ---------- |
| GB                  | 22 840 162 |
| England             | 20 318 501 |
| Wales               | 1 111 383  |
| Scotland            | 1 410 278  |

<a href="#epc">[back to top of EPC]</a>

### Cleansed EPC (EST) <a name="cleansed_epc"></a>

**IMPORTANT: We are not allowed to share this dataset without explicit permission by EST.**

In `inputs/EPC_data/EST_cleansed` you can find a cleansed version of the EPC data for England and Wales provided by EST.

EST selected a set of relevant features, cleaned them and got rid of erroneous values. They also identified duplicates. Both versions with and without deduplication are available as zipped files:

- EPC_England_Wales_cleansed.csv.zip
- EPC_England_Wales_cleansed_and_deduplicated.csv.zip (14.4 million samples)

This version does not include the most recent EPC data as it only contains entries until the first quarter of 2020. It also does not include any data on Scotland.

The cleansed set includs the following 45 features:

```
ROW_NUM, LMK_KEY, ADDRESS1, ADDRESS2, ADDRESS3, POSTCODE, BUILDING_REFERENCE_NUMBER, LOCAL_AUTHORITY, LOCAL_AUTHORITY_LABEL, CONSTITUENCY, COUNTY, LODGEMENT_DATE, FINAL_PROPERTY_TYPE, FINAL_PROP_TENURE, FINAL_PROPERTY_AGE, FINAL_HAB_ROOMS, FINAL_FLOOR_AREA, FINAL_WALL_TYPE, FINAL_WALL_INS, FINAL_RIR, FINAL_LOFT_INS, FINAL_ROOF_TYPE, FINAL_MAIN_FUEL, FINAL_SEC_SYSTEM, FINAL_SEC_FUEL_TYPE, FINAL_GLAZ_TYPE, FINAL_ENERGY_CONSUMPTION, FINAL_EPC_BAND, FINAL_EPC_SCORE, FINAL_CO2_EMISSIONS, FINAL_FUEL_BILL, FINAL_METER_TYPE, FINAL_FLOOR_TYPE, FINAL_FLOOR_INS, FINAL_HEAT_CONTROL, FINAL_LOW_ENERGY_LIGHTING, FINAL_FIREPLACES, FINAL_WIND_FLAG, FINAL_PV_FLAG, FINAL_SOLAR_THERMAL_FLAG, FINAL_MAIN_FUEL_NEW, FINAL_HEATING_SYSTEM
```

<a href="#epc">[back to top of EPC]</a>

### Preprocessed EPC Dataset (<a name="preprocessed_epc"></a>)

In `inputs/data/EPC/preprocessed_data/Q4_2021` you can find three different versions of preprocessed EPC data.

**Since the preprocessing depends on data cleaning and feature engineering algorithms that may change over time, the data in this folder should be considered a snapshot of the current status in September 2021. Ideally, you should always work with the output of the most recent preprocessing version.**

If you have no access to the S3 bucket and/or you would like to work with the most recent data, you can generate the preprocessed datasets from the raw data by executing the script in _preprocess_epc_data.py_ in `asf_core_data/pipeline/preprocessing`.

Run `python -m asf_core_data/pipeline/preprocessing/preprocess_epc_data.py`

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

**Note**: In order make up- and downloading more efficient, we zipped the files in `inputs/EPC_data/preprocessed_data` - so the filenames all end in _.zip_. The data loading script in `asf_core_data/pipeline/getters/epc_data.py` will handle the unzipping if necessary.

<a href="#epc">[back to top of EPC]</a>

### Downloading and Updating EPC Data <a name="epc_update"></a>

The original data for England and Wales is freely available [here](https://epc.opendatacommunities.org/). The data for Scotland can be downloaded from [here](https://statistics.gov.scot/resource?uri=http%3A%2F%2Fstatistics.gov.scot%2Fdata%2Fdomestic-energy-performance-certificates). You may need to sign in with your email address.

Note that the data is updated every quarter so in order to work with the most recent data, the EPC data needs to be downloaded and preprocessed. [Here](#recent_data) you can check when the data was updated last time. If you need to download or update the EPC data, follow the steps below.

<mark>Current version: Q4_2021</mark>

Below we describe the necessary steps to download and update the data. Don't worry! It's less complicated than it looks.

- Download most recent England/Wales data from [here](https://epc.opendatacommunities.org/https://epc.opendatacommunities.org/). The filename is _all-domestic-certificates.zip_.

- Download most recent Scotland data from [here](https://statistics.gov.scot/resource?uri=http%3A%2F%2Fstatistics.gov.scot%2Fdata%2Fdomestic-energy-performance-certificates). The filename is is of the format `D_EPC_data_2012-[year]Q[quarter]_extract_[month][year].zip`, for example `D_EPC_data_2012-2021Q4_extract_0721.zip`.

- Clear the folders `inputs/data/EPC/raw_data/England_Wales` and `inputs/data/EPC/raw_data/Scotland`. If there is raw EPC data from previous quarters/years, delete it or move it to a subfolder. Note that the most recently downlaoded data will include all the data from previous downloads so there should be no data loss when overwriting previous data.

- Move the downloaded zip files to the corresponding folders `England_Wales` or `Scotland`.

- Shorten the Scotland filename to `D_EPC_data.zip`.

- Note: The zipped Scotland data may not be processable by the Python package _ZipFile_ because of an unsupported compression method. This problem can be solved easily solved by unzipping and zipping the data manually, e.g. with the command `unzip`. Make sure the filename remains `D_EPC_data.zip`.

- Create a new folder in `outpus/EPC_data/preprocessed_data/` with the name pattern `Q[quarter]_[year]`, indicating when the data was updated last. This information will be displayed when downloading the data and is reflected in the original filename for Scotland. For example, _Q2_2021_ includes the data up to June 2021.

- Open the config file `config/base.yaml` and update all paths including `outputs/EPC_data/preprocessed_data/Q[quarter]_[year]` to match the folder created in the last step.

- Run the preprocessing script`python -m asf_core_data.pipeline.preprocessing.preprocess_epc_data` which generates the preprocessed data in folder `outputs/EPC_data/preprocessed_data/Q[quarter]_[year]`

  Note: The data preprocessing is optimised for the data collected in 2021 (Q4_2021). More recent EPC data may include values not yet covered by the current preprocessing algorithm (for example new construction age bands), possibly causing errors when excuting the script.
  These can usually be fixed easily so feel free to open an issue or submit a pull request.

- If you update the `/inputs` data on the S3 bucket, please let everyone know by creating an issue. For those without access to the S3 bucket, open an issue and we'll take care of it for you.

- Enjoy your updated EPC data.

<a href="#epc">[back to top of EPC]</a>

<a href="#top">[back to top]</a>

## MCS Data <a name="mcs"></a>

The MCS datasets contain information on MCS-certified heat pump **installations** and **installers**. These datasets are proprietary and cannot be shared outside of Nesta. Both datasets are provided quarterly by MCS and are stored in the **asf-core-data** bucket on S3. Information about the fields in this dataset can be found [here](https://docs.google.com/spreadsheets/d/1EKrQGZfeyVkQPJC05746vT0mWlGvK4vnzj5upi_fPBY/edit?usp=sharing) (accessible only by Nesta employees).

### How to use <a name="instructions"></a>

To pull installation data into a project, first install this repo as a package by adding this line to your project's `requirements.txt`, substituting `$BRANCH` for your desired branch name:

    asf_core_data@ git+ssh://git@github.com/nestauk/asf_core_data.git@$BRANCH

The data can then be pulled in using:

    from asf_core_data import get_mcs_installations

    installation_data = get_mcs_installations(epc_version=...)

Replace the `...` above with

- "none" to get just the MCS installation records
- "full" to get every MCS installation record joined to the property's full EPC history (one row for each installation-EPC pair) - installations without a matching EPC are kept, but with missing data in the EPC fields
- "most_relevant" to get every MCS installation joined to its "most relevant" EPC record (this being the latest EPC before the installation if one exists; otherwise, the earliest EPC after the installation) - installations without a matching EPC are kept, but with missing data in the EPC fields

To update the data on the `asf-core-data` S3 bucket, run:

    export COMPANIES_HOUSE_API_KEY="ADD_YOUR_API_KEY_HERE"

    from asf_core_data import generate_and_save_mcs

    generate_and_save_mcs()

which requires the user to have a Companies House API key:

- Create a developer account for Companies House API[here](https://developer.company-information.service.gov.uk/get-started);
- Create a new application [here](https://developer.company-information.service.gov.uk/manage-applications/add). Give it any name and description you want and choose the Live environment.
  Copy your API key credentials.

This requires processed EPC data to be saved locally as set out by the requirements in the section above.

To run checks on raw installations data:

    from asf_core_data import test_installation_data

    test_installation_data(filename)

### Installations <a name="mcs_installations"></a>

The MCS **installations** dataset contains one record for each MCS certificate. In almost all cases, each MCS certificate corresponds to a single heat pump system installation (which in some cases may feature multiple heat pumps). The dataset contains records of both domestic and non-domestic installations. Features in the dataset include

- characteristics of the property: address, heat and water demand, alternative heating system
- characteristics of the installed heat pump: model, manufacturer, capacity, flow temperature, SCOP
- characteristics of the installation: commissioning date, overall cost, installer name, whether or not the installation is eligible for RHI
- characteristics of the certificate: version number, certification date

For further information about the data collection process see [this doc](https://docs.google.com/document/d/1uuptYecUfIm1Dxy1iuw19xgareZPzg_WP4M7J80mTgc/edit?usp=sharing) (assessible only to Nesta employees).

### Installers <a name="mcs_installers"></a>

The MCS **installers** dataset contains one record for each MCS-certified installer. Features in the dataset include

- name and address
- technologies installed

### Merging with EPC <a name="mcs_epc_merging"></a>

**asf_core_data/pipeline/mcs** contains functions for processing the raw MCS data and joining it with EPC data. In particular, running **generate_mcs_data.py** will process and save three different versions of the data to S3:

1. Cleaned MCS installation data, with one row for each **installation**
2. As in 1., with EPC data fully joined: when a property also appears in EPC data, a row is included for each EPC (so the full EPC history of the property appears in the data)
3. As in 2., filtered to the "most relevant" EPC which aims to best reflect the status of the property at the time of the installation: the latest EPC before the installation if one exists, otherwise the earliest EPC after the installation

Further details about the processing and joining of the data are provided within asf_core_data/pipeline.

<a href="#top">[back to top]</a>

</mark>

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
