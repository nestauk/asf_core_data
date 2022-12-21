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

### Downloading and/or Updating EPC Data <a name="epc_update"></a>

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
