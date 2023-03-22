"""
Script for pre-processing historical MCS heat pump installer company data.
- Dropping duplicate instances (if they exist);
- Renaming columns;
- Removing instances from test account(s);
- Joining installation and design variables;
- Adding certification body information.
- Adding instances for installers with installations but with info missing from installers table;
- Creating flag variables for whether an installer is certified for a certain technology;
- Geocoding data by mapping postcode to latitude and longitude values;
- Creating a variable that uniquely identifies installers;

Note that:
- installer name/ company name/ installation company name are used interchangebly throughout the whole script;
- the dataset(s) being processed contain information about installers who are or have been MCS certified for heat
pump installations. Hence, even if we have information about other types of certification (e.g. biomass) this
does not represent the whole dataset for those types of installers.

To run script, in activated conda environment do:

1) To process the raw historical MCS installers data received on the 7th of Feb 2023:
`export COMPANIES_HOUSE_API_KEY="YOUR_API_KEY"`
`python3 process_historical_mcs_installers.py`
OR
`python3 process_historical_mcs_installers.py -api_key "YOUR_API_KEY"`

2) To process another version of the raw historical installers data
(where YYYYMMDD corresponds to the date the data was shared by MCS in the data dumps Google Drive folder, e.g. 20230207):
`export COMPANIES_HOUSE_API_KEY="YOUR_API_KEY"`
`python3 process_historical_mcs_installers.py -raw_historical_installers_filename "raw_historical_mcs_installers_YYYYMMDD.xlsx" -raw_historical_installations_filename "raw_historical_mcs_installations_YYYYMMDD.xlsx"`
OR
`python3 process_historical_mcs_installers.py -api_key "YOUR_API_KEY" -raw_historical_installers_filename "raw_historical_mcs_installers_YYYYMMDD.xlsx" -raw_historical_installations_filename "raw_historical_mcs_installations_YYYYMMDD.xlsx"`

Note that the processing of historical installers is included the script that processes MCS data:
python3 asf_core_daa/pipeline/mcs/generate_mcs_data.py
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime
from argparse import ArgumentParser
from asf_core_data.config import base_config
from asf_core_data.getters.data_getters import load_s3_data, save_to_s3
from asf_core_data.getters.mcs_getters.get_mcs_installers import (
    get_raw_historical_installers_data,
)
from asf_core_data.getters.mcs_getters.get_mcs_installations import (
    get_raw_historical_installations_data,
)
from asf_core_data.pipeline.mcs.process.process_mcs_utils import (
    rename_columns,
    match_companies_house,
    clean_company_name,
    geocode_postcode,
    deal_with_versions_of_trading_as,
    from_list_to_dictionary,
    map_position_of_subset_items,
    position_to_value,
)


def basic_preprocessing_of_installations(raw_historical_installations: pd.DataFrame):
    """
    Applies basic preprocessing to raw installations data:
    - Renaming columns (lower case, spaces to underscore, etc.);
    - Extracting certification body and certification number info from original var.
    Args:
        raw_historical_installations: raw historical installations table
    """

    raw_historical_installations.columns = rename_columns(
        raw_historical_installations.columns
    )

    raw_historical_installations[
        ["certification_body", "installation_company_mcs_number"]
    ] = raw_historical_installations["installation_company_mcs_number"].str.split(
        " ", expand=True
    )

    raw_historical_installations[
        "installation_company_mcs_number"
    ] = raw_historical_installations["installation_company_mcs_number"].astype(int)


def drop_instances_test_accounts(
    data: pd.DataFrame, company_name_var: str
) -> pd.DataFrame:
    """
    Drops instances from accounts used for testing by MCS and returns updated data.

    Args:
        data: installations/installers data
        company_name_var: name of the variable in data containing the company name
    Returns:
        Updated installations/installers data
    """
    test_accounts = ["Sharma Solar Thermal Limited 1"]
    data = data[~data[company_name_var].isin(test_accounts)]
    return data


def join_installation_and_design_vars(data: pd.DataFrame):
    """
    Joins together installation and design date variables and drops original variables.
    E.g.
    Original variables: air_source_hp_installation_start_date, air_source_hp_design_start_date
    New variable: air_source_hp_start_date

    If installation start/end date exist, we keep installation date. Otherwise, we use the design date.

    Args:
        data: historical installer data
    """

    installation_cols = [col for col in data.columns if "installation" in col]
    for col in installation_cols:
        new_date_var = col.replace("installation_", "")
        correspondent_design_var_name = col.replace("installation", "design")
        data[new_date_var] = data.apply(
            lambda x: x[col]
            if not pd.isnull(x[col])
            else x[correspondent_design_var_name],
            axis=1,
        )

        data.drop([col, correspondent_design_var_name], axis=1, inplace=True)


def recompute_full_adress(
    date_creation: datetime, first_commisioning_date: datetime, full_address: str
) -> str:
    """
    If company date of creation happens before the first HP comissioning date
    then we keep the full address (good match), otherwise address to None (meaning a bad
    fuzzy match with Companies House API).

    Args:
        date_creation: date company was created, according to match from Companies House
        first_comissioning_date: first HP comissioning date according to HP installations data
        full_address: full address retrieved from Companies House match
    Returns:
        recomputed full address
    """
    if pd.isnull(date_creation) or pd.isnull(first_commisioning_date):
        return full_address
    return full_address if date_creation <= first_commisioning_date else None


def get_missing_installers_info(
    installations: pd.DataFrame, installers: pd.DataFrame, companies_house_api_key: str
) -> pd.DataFrame:
    """
    Gets information about installers missing from historical installers table, i.e.
    installers with installations but with no info in installers table.
    Information comes from two sources: 1) historicaL installations table and from 2) Companies House API.

    1) From historical installations data:
    - Start by merging installations and installers to identify missing installers info;
    - Get company name, certification number;
    - Get certification body, technology type and min and max comission dates, which are
    used to infer certification start and end dates.

    2) From Companies House API:
    - Full address;
    - Company date of creation (comparing this to first HP commission date serves as a check for a good match).

    Args:
        installations: historical installations table
        installers: historical installations table
        companies_house_api_key: Companies house personal API key

    Returns:
        missing installers information dataframe.
    """

    # Merge the two datasets on pair (company_name, certificate_number)
    merged_data = installations.merge(
        right=installers[["company_name", "mcs_certificate_number"]],
        how="left",
        left_on=["installation_company_name", "installation_company_mcs_number"],
        right_on=["company_name", "mcs_certificate_number"],
    )

    # Missing installers are those for which there is no match i.e. company_name/mcs_certificate_number missing
    missing_installers = merged_data[
        pd.isnull(merged_data["company_name"])
        | pd.isnull(merged_data["mcs_certificate_number"])
    ]

    # Info we get about missing installers from the installations table
    missing_installers = missing_installers[
        [
            "installation_company_name",
            "installation_company_mcs_number",
            "certification_body",
            "technology_type",
            "commissioning_date",
        ]
    ]

    # We infer the certification start and end dates from the first and last commisioning dates for each technology
    missing_installers = (
        missing_installers.groupby(
            [
                "installation_company_name",
                "installation_company_mcs_number",
                "certification_body",
                "technology_type",
            ]
        )
        .agg(
            start_date=("commissioning_date", np.min),
            end_date=("commissioning_date", np.max),
        )
        .unstack()
    )

    # Renaming columns to match those in the historical installers table e.g. air_source_hp_start_date
    new_column_names = [
        rename_columns(missing_installers.columns.get_level_values(1))[i]
        + "_"
        + missing_installers.columns.get_level_values(0)[i]
        for i in range(len(missing_installers.columns))
    ]
    missing_installers.columns = new_column_names
    missing_installers.reset_index(inplace=True)

    # original_record is set to False as these are not originally in the installers table
    missing_installers["original_record"] = [
        False for i in range(len(missing_installers))
    ]

    # fuzzy match installer companies NOT in installer company data using companies house API to get address info
    missing_installers[["full_address", "date_of_creation"]] = missing_installers.apply(
        lambda x: match_companies_house(
            x["installation_company_name"], companies_house_api_key
        ),
        axis=1,
    )

    # date_of_creation type change: str -> datetime
    missing_installers["date_of_creation"] = missing_installers[
        "date_of_creation"
    ].apply(lambda x: datetime.strptime(x, "%Y-%m-%d") if not pd.isnull(x) else x)

    # We only keep address info if the company creation date is before the first comissioning date (serves as a check)
    start_date_cols = [col for col in missing_installers if "start_date" in col]
    missing_installers["first_commissioning_date"] = missing_installers[
        start_date_cols
    ].min(axis=1)

    missing_installers["full_address"] = missing_installers.apply(
        lambda x: recompute_full_adress(
            x["date_of_creation"], x["first_commissioning_date"], x["full_address"]
        ),
        axis=1,
    )

    # extract address_1 and postcode from full_address variable
    missing_installers["address_1"] = missing_installers["full_address"].apply(
        lambda x: x.split(",")[0] if x is not None else None
    )
    missing_installers["postcode"] = missing_installers["full_address"].apply(
        lambda x: x.split(",")[-1].strip() if x is not None else None
    )

    # rename columns to match installers table
    missing_installers.rename(
        columns={
            "installation_company_name": "company_name",
            "installation_company_mcs_number": "mcs_certificate_number",
        },
        inplace=True,
    )

    # dropping unecessary variables
    missing_installers.drop(
        columns=["date_of_creation"],
        inplace=True,
    )

    return missing_installers


def update_full_address(
    address_1: str,
    address_2: str,
    town: str,
    county: str,
    postcode: str,
    original_record: str,
    full_address: str,
) -> str:
    """
    Computes full_address by putting together the different lines in address
    (address_1, address_2, town, county and postcode) for original records.

    Args:
        address_1: first line of address
        address_2: second line of address
        town: town
        county: county
        postcode: postcode
        original_record: True if record originally in installers table
    Returns:
        full adress
    """
    if original_record:
        full_address = []
        for address_part in [address_1, address_2, town, county, postcode]:
            if not pd.isnull(address_part):
                full_address.append(address_part)
        return ", ".join(full_address)

    # if not original record, we have the full address from Companies House
    return full_address


def create_certified_flags(data: pd.DataFrame):
    """
    Updates historical installers data by creating flags representing whether
    installer is certified or not for a certain technology.

    If a start date exists, then it means installer is certified.

    Args:
        data: historical installers data
    """

    # start date variables for all tech types e.g. air_source_hp_start_date
    start_date_cols = [col for col in data.columns if "start_date" in col]

    for col in start_date_cols:
        # example of flag_var: air_source_hp_certified
        flag_var = col.replace("start_date", "certified")
        data[flag_var] = ~pd.isnull(data[col])


def dictionary_mapping_trading_as_company_names(
    trading_as_companies: pd.DataFrame,
) -> dict:
    """
    Creates a dictionary mapping company names containing the "trading as" in the name.

    As an example, if we have 3 original installer names:
    - "Company A"
    - "Company B"
    - "Company A trading as Company B"
    They should all be identified as the same company, e.g. identified as "Company A".

    Args:
        trading_as_companies: dataframe containing a processed_company_name variable where
        company names contain the "trading as" expression.
    Returns:
        A dictionary mapping company names.
    """

    # For each instance in data, it creates a list with all possible versions of each company names
    # splitting string by "trading as". E.g. "A trading as B" -> ["A", "B"]
    trading_as_companies["list_of_companies"] = trading_as_companies[
        "processed_company_name"
    ].str.split(" trading as ")

    # Adding full name to the list as well : ["A", "B", "A trading as B"]
    trading_as_companies["list_of_companies"] = trading_as_companies.apply(
        lambda x: x["list_of_companies"] + [x["processed_company_name"]], axis=1
    )

    # Transforms the list into a dictionary where each key is matched to the first value in the list:
    # ["A", "B", "A trading as B"] -> {"B":"A", "A trading as B":"A"}
    trading_as_companies["dictionary_of_companies"] = trading_as_companies[
        "list_of_companies"
    ].apply(from_list_to_dictionary)

    # Transforms the pd.Series trading_as_companies["dictionary_of_companies"] into one dictionary
    trading_as_dictionary = {
        k: v
        for d in trading_as_companies["dictionary_of_companies"]
        for k, v in d.items()
    }

    return trading_as_dictionary


def map_installers_same_location_different_id(
    postcodes: list, installer_data: pd.DataFrame
) -> dict:
    """
    Returns a dictionary with a mapping between company unique IDs for companies that have the same
    address but are not originally identified as the same company.

    Given companies that have the same company postcode and address_1, but different company unique IDs,
    we create a map between the IDs.

    Args:
        postcodes: list of postcodes corresponding to multiple company unique IDs
        installer_data: installer data
    Returns:
        A dictionary with a mapping between company unique IDs that represent the same company.
    """

    # Installer data with potential for mapping
    installers_not_matched = installer_data[installer_data["postcode"].isin(postcodes)]

    # Creates a comparable address_1 by removing any punctuation and spaces
    # e.g. both "Unit 1, ABC Road" and "Unit1 ABC Road" will translate to "unit1abcroad"
    installers_not_matched["comparable_address_1"] = (
        installers_not_matched["address_1"]
        .str.lower()
        .str.translate(str.maketrans("", "", "()-,/."))
        .str.replace(" ", "")
    )

    # list of comparable addresses and unique IDs
    installers_not_matched = installers_not_matched.groupby("postcode").agg(
        comparable_address_1=("comparable_address_1", list),
        company_unique_id=("company_unique_id", list),
    )

    # Discard instances when postcode has only one instance associated to it
    installers_not_matched = installers_not_matched[
        installers_not_matched["comparable_address_1"].apply(len) > 1
    ]

    # Creates a mapping between indices of comparable addresses that are the same
    installers_not_matched["indices_mapping"] = installers_not_matched[
        "comparable_address_1"
    ].apply(map_position_of_subset_items)

    # Maps the position to the company IDs so that company IDs corresponding
    # to the same address are mapped together
    installers_not_matched["matches"] = installers_not_matched.apply(
        lambda x: position_to_value(x["company_unique_id"], x["indices_mapping"]),
        axis=1,
    )

    # Transforms the pd.Series installers_not_matched["matches"] into one dictionary
    new_matches = {
        k: v for d in installers_not_matched["matches"] for k, v in d.items()
    }

    return new_matches


def create_installer_unique_id(installer_data: pd.DataFrame):
    """
    Creates a string unique identifier for each installer company, since multiple company names might
    refer to the same installer.
    E.g. "Company A Ltd", "Company A Limited", "Company A (old account), "Company A t/a Company B"
    all refer to the same company which will be identified by the unique ID "company a".

    Identifiers are created by transforming the original company names:
    - Cleaning the company name (strings to lowe case, removing stopwords and punctuation);
    - Merge all different "trading as" expressions together;
    - Creating a dictionary to map companies trading as other companies to be identified as the same;
    - Creating a dictionary to map companies with slightly different names with address in the same location;

    Args:
        installer_data: historical installers data
    """
    # Cleaning company name
    installer_data["processed_company_name"] = installer_data["company_name"].apply(
        clean_company_name
    )

    # Dealing with different versions of "trading as"
    installer_data["processed_company_name"] = installer_data[
        "processed_company_name"
    ].apply(deal_with_versions_of_trading_as)

    # Unique companies containing "trading as" in the name
    trading_as_companies = installer_data[
        installer_data["processed_company_name"].str.contains("trading as")
    ][["processed_company_name"]]
    trading_as_companies.drop_duplicates("processed_company_name", inplace=True)

    # Creating dicitionary to map "trading as" companies to the same company name
    trading_as_dictionary = dictionary_mapping_trading_as_company_names(
        trading_as_companies
    )

    # Applying the mapping created above to create the company unique ID
    installer_data["company_unique_id"] = installer_data[
        "processed_company_name"
    ].apply(
        lambda x: trading_as_dictionary[x] if x in trading_as_dictionary.keys() else x
    )

    # Dropping processed_company_name as no longer needed
    installer_data.drop("processed_company_name", axis=1, inplace=True)

    # Companies have the same location but not currently identified as being the same
    same_postcode_no_match = installer_data.groupby("postcode", as_index=False)[
        ["company_unique_id"]
    ].nunique()
    same_postcode_no_match = same_postcode_no_match[
        same_postcode_no_match["company_unique_id"] > 1
    ]["postcode"].unique()

    # New unique ID matches based on companies with same location
    new_matches = map_installers_same_location_different_id(
        same_postcode_no_match, installer_data
    )
    installer_data["company_unique_id"] = installer_data["company_unique_id"].apply(
        lambda x: new_matches[x] if x in new_matches.keys() else x
    )


def add_certification_body_info(
    installer_data: pd.DataFrame, installations_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Adds certification body information to each instance of the historical installers table.

    Historical installers data does not come with certification body information, only with certification number.
    Company name and the certification number are used to merge and match to the installations table and get the
    certification body from there.

    Args:
        installer_data: historical installers data
        installations_data: historical installations data
    Returns:
        Updated table of installers
    """

    installations_match_vars = [
        "installation_company_name",
        "installation_company_mcs_number",
    ]
    installers_match_vars = ["company_name", "mcs_certificate_number"]

    # dataframe with unique pairs (installation_company_name, installation_company_mcs_number)
    # and corresponding certification body
    unique_pairs_in_installations = installations_data.drop_duplicates(
        installations_match_vars
    )[
        [
            "installation_company_name",
            "installation_company_mcs_number",
            "certification_body",
        ]
    ]

    # Merge installer with installation data to add the certification_body to the installer data
    installer_data = installer_data.merge(
        unique_pairs_in_installations,
        how="left",
        right_on=installations_match_vars,
        left_on=installers_match_vars,
    )

    # Dropping installation variables used to merge and match
    installer_data.drop(columns=installations_match_vars, inplace=True)

    return installer_data


def preprocess_historical_installers(
    raw_historical_installers: pd.DataFrame,
    raw_historical_installations: pd.DataFrame,
    geographical_data: pd.DataFrame,
    companies_house_api_key: str,
) -> pd.DataFrame:
    """
    Pre-processes raw historical installers data by:
    - Dropping duplicate instances (if they exist);
    - Renaming columns;
    - Removing instances from test account(s);
    - Joining installation and design variables;
    - Adding certification body information;
    - Adding instances for installers with installations but with info missing from installers table;
    - Creating flag variables for whether an installer is certified for a certain technology;
    - Geocoding data by mapping postcode to latitude and longitude values;
    - Creating a variable that uniquely identifies installers.

    Args:
        raw_historical_installers: raw historical installers data
        raw_historical_installations: raw historical installations data
        geographical_data: geographical data with postcode, latitude and longitude information
        companies_house_api_key: Companies House API key
    Returns:
        Processed historical installers data
    """

    # Apply basic preprocessing to table of installations
    basic_preprocessing_of_installations(raw_historical_installations)

    # Dropping duplicate lines
    raw_historical_installers.drop_duplicates(inplace=True)

    # Renaming columns
    raw_historical_installers.columns = rename_columns(
        raw_historical_installers.columns
    )

    # Drop instances from test accounts
    raw_historical_installations = drop_instances_test_accounts(
        raw_historical_installations, "installation_company_name"
    )
    raw_historical_installers = drop_instances_test_accounts(
        raw_historical_installers, "company_name"
    )

    # Joining installation and design variables
    join_installation_and_design_vars(raw_historical_installers)

    # flag "original_records" (if originally in installers or inputed from installations)
    raw_historical_installers["original_record"] = [
        True for i in range(len(raw_historical_installers))
    ]

    # Certification body information
    raw_historical_installers = add_certification_body_info(
        raw_historical_installers, raw_historical_installations
    )

    # Get info about installers with installations but with info missing from installers
    missing_installers = get_missing_installers_info(
        raw_historical_installations, raw_historical_installers, companies_house_api_key
    )

    raw_historical_installers = pd.concat(
        [raw_historical_installers, missing_installers]
    )
    raw_historical_installers.reset_index(drop=True, inplace=True)

    # Updating full_address variable
    raw_historical_installers["full_address"] = raw_historical_installers.apply(
        lambda x: update_full_address(
            x["address_1"],
            x["address_2"],
            x["town"],
            x["county"],
            x["postcode"],
            x["original_record"],
            x["full_address"],
        ),
        axis=1,
    )

    # Creating flag for whether certified for a certain technology
    create_certified_flags(raw_historical_installers)

    # Geocoding data
    raw_historical_installers = geocode_postcode(
        raw_historical_installers, geographical_data
    )

    # Create installer unique ID
    create_installer_unique_id(raw_historical_installers)

    return raw_historical_installers[
        base_config.processed_historical_installers_columns_order
    ]


def create_argparser() -> ArgumentParser:
    """
    Creates an argument parser that can receive the following arguments:
    - raw historical installers file name, as `raw_historical_installers_filename`
    - raw historical installations file name, as `raw_historical_installations_filename`
    - companies house api key, as `api_key`
    """
    parser = ArgumentParser()

    parser.add_argument(
        "--raw_historical_installers_filename",
        help="Raw historical installers file name",
        default="raw_historical_mcs_installers_20230207.xlsx",
        type=str,
    )
    parser.add_argument(
        "--raw_historical_installations_filename",
        help="Raw historical installations file name",
        default="raw_historical_mcs_installations_20230119.xlsx",
        type=str,
    )
    parser.add_argument(
        "-key",
        "--api_key",
        help="Companies House API key",
        default=os.environ.get("COMPANIES_HOUSE_API_KEY"),
        type=str,
    )

    return parser


if __name__ == "__main__":
    s3_bucket_name = base_config.BUCKET_NAME

    parser = create_argparser()
    args = parser.parse_args()

    # Loading geodata
    uk_geo_path = base_config.POSTCODE_TO_COORD_PATH
    uk_geo_data = load_s3_data(s3_bucket_name, uk_geo_path)

    # Getting raw installations and installers data from S3
    raw_historical_installers = get_raw_historical_installers_data(
        args.raw_historical_installers_filename
    )
    raw_historical_installations = get_raw_historical_installations_data(
        args.raw_historical_installations_filename
    )

    # Process raw historical installers data
    processed_historical_installers = preprocess_historical_installers(
        raw_historical_installers=raw_historical_installers,
        raw_historical_installations=raw_historical_installations,
        geographical_data=uk_geo_data,
        companies_house_api_key=args.api_key,
    )

    # Saving processed data to S3
    processed_data_path = base_config.PREPROCESSED_MCS_HISTORICAL_INSTALLERS_FILE_PATH
    # date when data was shared by MCS in MCS data dumps
    date = args.raw_historical_installers_filename.split("installers_")[1].split(
        ".xlsx"
    )[0]

    save_to_s3(
        s3_bucket_name,
        processed_historical_installers,
        processed_data_path.format(date),
    )
