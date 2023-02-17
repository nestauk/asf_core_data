"""Processing historical MCS installer company data.

Installer name/ company name/ installation company name are used interchangebly throughout the whole script.

To run script, (in activated conda environment) python process_historical_mcs_installers.py -key API KEY
"""

import pandas as pd
import numpy as np
import requests
import random
import time
import os
from datetime import datetime
from argparse import ArgumentParser
from asf_core_data.config import base_config
from asf_core_data.getters.data_getters import s3, load_s3_data, save_to_s3
from asf_core_data.getters.mcs_getters.get_historical_mcs_installers import (
    get_raw_historical_mcs_installers,
)
from asf_core_data.getters.mcs_getters.get_mcs_installations import (
    get_raw_historical_installations_data,
)


def basic_preprocessing_of_installations(raw_historical_installations: pd.DataFrame):
    """
    Applies basic preprocessing to raw installations to make it easier to work with:
    - Renaming columns (lower case, spaces to underscore, etc.);
    - Extracting certification body and certification number info from original var
    Args:
        raw_historical_installations: raw historical installations table
    """

    raw_historical_installations.columns = rename_columns(
        raw_historical_installations.columns
    )

    raw_historical_installations["certification_body"] = (
        raw_historical_installations["installation_company_mcs_number"]
        .str.split(" ")
        .str[0]
    )

    raw_historical_installations["installation_company_mcs_number"] = (
        raw_historical_installations["installation_company_mcs_number"]
        .str.split(" ")
        .str[1]
        .astype(int)
    )


def rename_columns(cols: list) -> list:
    """
    Renames a list of strings, e.g. a list of column names, by:
    - applying lower case
    - replacing "heat pump" by "hp"
    - replacing " " and "/" by "_"
    - correcting the word address.

    Args:
        cols: original column names
    Returns:
        Renamed column names
    """
    cols = [c.lower() for c in cols]
    changes_dict = {"heat pump": "hp", " ": "_", "/": "_", "adddress": "address"}
    for item in changes_dict.keys():
        cols = [c.replace(item, changes_dict[item]) for c in cols]

    return cols


def joining_installation_and_design_vars(data: pd.DataFrame):
    """
    Joins together installation and design date variables and drops original variables.

    If installation date exist, we keep installation date. Otherwise, we keep design date.

    Args:
        data: historical installer data
    """

    installation_cols = [col for col in data.columns if "installation" in col]
    for col in installation_cols:
        date_var = col.replace("installation_", "")
        correspondent_design_var_name = col.replace("installation", "design")
        data[date_var] = data.apply(
            lambda x: x[col]
            if not pd.isnull(x[col])
            else x[correspondent_design_var_name],
            axis=1,
        )

        data.drop([col, correspondent_design_var_name], axis=1, inplace=True)


def deal_with_versions_of_trading_as(installer_name: str) -> str:
    """
    Replaces several expressions representing "trading as" by the expression
    "trading as" in the installer name.

    Args:
        installer_name: installer name

    Returns:
        updated installer name
    """
    versions = {
        "t/a": "trading as",
        " ta ": " trading as ",
        "a trading name of": "trading as",
        "the trading name of": "trading as",
    }
    for item in versions:
        installer_name = installer_name.replace(item, versions[item])

    return installer_name


# copied from process_mcs_utils.py and changed slightly
def match_companies_house(company_name: str, api_key: str) -> pd.Series:
    """
    Fuzzy matches company name with company house API and
    returns Companies House information: company full address and date of creation.

    Args:
        company_name: Company name to query company house API with.
        api_key: API key to call live company house API endpoint.
    Returns:
        company_info: pandas Series with 2 values, full address and creation date
    """

    # E.g. "Flo Group LTD T/A Flo Renewables" -> "flo renewables"
    company_name = company_name.lower()
    if "trading as" in company_name:
        company_name = deal_with_versions_of_trading_as(company_name).split(
            " trading as "
        )[1]

    base_url = f"https://api.company-information.service.gov.uk/search/companies?q={company_name}"

    response = requests.get(base_url, auth=(api_key, ""))

    # developer guidelines state you can make 600 requests per 5 mins
    # this translates into 1 request every 0.5 seconds
    time.sleep(0.5)

    response_status_code = response.status_code

    if response_status_code == 200:
        company_data = response.json()
        if (
            (company_data is not None)
            and ("items" in company_data.keys())
            and len(company_data["items"]) > 0
        ):
            # return that for the closest match, the one found in index 0
            if ("address_snippet" in company_data["items"][0].keys()) and (
                "date_of_creation" in company_data["items"][0].keys()
            ):
                return pd.Series(
                    [
                        company_data["items"][0]["address_snippet"],
                        company_data["items"][0]["date_of_creation"],
                    ]
                )
            elif "address_snippet" in company_data["items"][0].keys():
                return pd.Series(
                    [
                        company_data["items"][0]["address_snippet"],
                        None,
                    ]
                )
            elif "date_of_creation" in company_data["items"][0].keys():
                return pd.Series(
                    [
                        None,
                        company_data["items"][0]["date_of_creation"],
                    ]
                )
            else:
                return pd.Series([None, None])
        else:
            return pd.Series([None, None])
    else:
        if response_status_code >= 400 and response_status_code < 500:
            raise Exception(
                "Cannot get data, the program will stop!\nHTTP {}: {}".format(
                    response_status_code, response.text
                )
            )
        sleep_seconds = random.randint(5, 60)
        print(
            "Cannot get data, your program will sleep for {} seconds...\nHTTP {}: {}".format(
                sleep_seconds, response_status_code, response.text
            )
        )
        time.sleep(sleep_seconds)
        return match_companies_house(company_name, api_key)


def recompute_full_adress(date_creation, first_commisioning_date, full_address):
    """
    If company date of creation happens before the first HP comissioning date
    then we keep the full address (good match), otherwise address to None (bad batch).
    Args:
        date_creation: date company was created, according to match from Companies House
        first_comissioning_date: first HP comissioning date according to HP installations data
        full_address: full address retrieved from Companies House match
    Returns:
        recomputed full address.
    """
    if pd.isnull(date_creation) or pd.isnull(first_commisioning_date):
        return full_address
    else:
        return full_address if date_creation <= first_commisioning_date else None


def get_missing_installers_info(
    installations: pd.DataFrame, installers: pd.DataFrame, companies_house_api_key: str
) -> pd.DataFrame:
    """
    Gets information about installers missing from historical installers table, i.e.
    installers with installations but with no info in installers table.
    Information comes from two sources: 1) historicak installations table and from 2) Companies House API.

    1)  historical installations:
    - Start by merging installations and installers to identify missing installers info;
    - Get company name, certification number, certification body, technology type and min
    and max comission dates (to then infer certification start and end dates)

    2) Companies House API:
    - Full address;
    - Company date of creation (comparing this to min HP commission date serves as a check for a good match)

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

    # Info we know about missing installers from the installations table
    missing_installers = missing_installers[
        [
            "installation_company_name",
            "installation_company_mcs_number",
            "certification_body",
            "technology_type",
            "commissioning_date",
        ]
    ]

    # We infer the certification start and end dates from the min and max commisioning dates for each technology
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
        {
            "installation_company_name": "company_name",
            "installation_company_mcs_number": "mcs_certificate_number",
        },
        axis=1,
        inplace=True,
    )

    # dropping unecessary variables
    missing_installers.drop(
        ["date_of_creation"],
        axis=1,
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
    Updates full_address variable with full address for original records (when original_records is True).

    Args:
        address_1: first line of address
        address_2: second line of address
        town: town
        county: county
        postcode: postcode
        original_record: True if record originally in
    Returns:
        full adress
    """
    if original_record:
        temp = address_1
        if not pd.isnull(address_2):
            temp = temp + address_2 + ", "
        if not pd.isnull(town):
            temp = temp + town + ", "
        if not pd.isnull(county):
            temp = temp + county + ", "
        if not pd.isnull(postcode):
            temp = temp + postcode + ", "
        return temp[:-2]
    else:  # if not original record, we have the full address from Companies House
        return full_address


def create_certified_flags(data: pd.DataFrame):
    """
    Updates historical installers data by creates flags representing whether
    installer is certified or not for a certain technology.

    If a start date exists, then if means installer is certified.

    Args:
        data: historical installers data
    """

    # start date variables for all tech types e.g. air_source_hp_start_date
    start_date_cols = [col for col in data.columns if "start_date" in col]

    for col in start_date_cols:
        # example of flag_var: air_source_hp_certified
        flag_var = col.split("start_date")[0] + "certified"
        data[flag_var] = ~pd.isnull(data[col])


# copied from process_mcs_utils.py and changed slightly
def clean_company_name(company_name: str) -> str:
    """
    Cleans installation company name by:
        - company name to lower case;
        - removes punctuation;
        - removing "company" stopwords like ltd, limited;

    Args:
        company_name: Name of company to clean.
    Returns:
        company_name: Cleaned company name.
    """
    company_stopwords = ["ltd", "ltd.", "limited", "limited.", "old", "account"]

    company_name = company_name.lower().translate(str.maketrans("", "", "()-,/.&"))
    company_name = [
        comp for comp in company_name.split() if comp not in company_stopwords
    ]

    return " ".join(company_name)


def from_list_to_dictionary(list_values):
    """
    Transforms a list into a dictionary where each key is matched to the first
    value in the list.
    Arg:
        list_values: a list of values e.g. ["Company A", "Company B", "Company A trading as Company B"]
    Returns:
        Dictionary mapping all values to the first one
        e.g. {"Company B":"Company A", "Company A trading as Company B": "Company A"}
    """
    dictionary = dict()
    for value in list_values:
        dictionary[value] = list_values[0]
    return dictionary


def dictionary_mapping_trading_as_company_names(
    trading_as_companies: pd.DataFrame,
) -> dict:
    """
    Creates a dictionary mapping company names containing the "trading as" in the name.

    As an example, if we have 3 original installer names:
    - "Company A"
    - "Company B"
    - "Company A trading as Company B"
    They should all be mapped identified the same company, e.g. identified as "Company A".

    Args:
        trading_as_companies: dataframe containing a processed_company_name variable where
        company names contain the "trading as" expression.
    Returns:
        A dictionary mapping company names.
    """

    # List with all possible company names: "A trading as B" -> ["A", "B"]
    trading_as_companies["list"] = trading_as_companies[
        "processed_company_name"
    ].str.split(" trading as ")
    # Adding full name as well : ["A", "B", "A trading as B"]
    trading_as_companies["list"] = trading_as_companies.apply(
        lambda x: x["list"] + [x["processed_company_name"]], axis=1
    )

    # Transforms the list into a dictionary where each key is matched to the first value in the list:
    # ["A", "B", "A trading as B"] -> {"B":"A", "A trading as B":"A"}
    trading_as_companies["dictionary"] = trading_as_companies["list"].apply(
        from_list_to_dictionary
    )

    # Transforms the pd.Series trading_as_companies["dictionary"] into one dictionary
    trading_as_dictionary = {
        k: v for d in trading_as_companies["dictionary"] for k, v in d.items()
    }

    return trading_as_dictionary


def matching_position_of_subset_items(list_of_strings: list) -> dict:
    """
    Returns a dictionary matching the position of strings in a list which are a subset of each other.
    Example
    set_of_strings = {"company a ltd", "a ltd", "a"}
    Output: {0: 2, 1: 2} (note that initially, 0 is mapped to 1 but then that's overwritten by 0 being mapped to 2)

    Args:
        list_of_strings: a list of strings
    Returns:
        A dictionary matching the position of strings that are subset of another.
    """
    return {
        i: j
        for i in range(len(list_of_strings) - 1)
        for j in range(i + 1, len(list_of_strings), 1)
        if (list_of_strings[i] in list_of_strings[j])
        or (list_of_strings[j] in list_of_strings[i])
    }


def position_to_value(list_of_strings: list, indices_dict: dict) -> dict:
    """
    Returns dictionary resulting from mapping a dictionary with indices as key-value pairs to a list of strings.
    Args:
        list_of_strings: list of strings, e.g. ["a", "b", "c]
        indices_dict: dictionary of indices, e.g. {0:2, 1:2}
    Returns:
        The mapped dictionary e.g. {"a":"c", "b":"c"}

    """
    return {
        list_of_strings[key]: list_of_strings[indices_dict[key]]
        for key in indices_dict.keys()
    }


def match_places_same_location_different_id(postcodes, installer_data):
    """
    Returns a dictionary with a mapping between company unique IDs.

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

    # removing any punctuation and spaces: e.g. both "Unit 1, ABC Road" and "Unit1 ABC Road" will translate to "unit1abcroad"
    installers_not_matched["comparable_address_1"] = (
        installers_not_matched["address_1"]
        .str.lower()
        .str.translate(str.maketrans("", "", "()-,/."))
        .str.replace(" ", "")
    )

    installers_not_matched = installers_not_matched.groupby("postcode").agg(
        comparable_address_1=("comparable_address_1", list),
        company_unique_id=("company_unique_id", list),
    )

    possible_adresses_and_names = installers_not_matched[
        installers_not_matched["comparable_address_1"].apply(len) > 1
    ]
    possible_adresses_and_names["indices_mapping"] = possible_adresses_and_names[
        "comparable_address_1"
    ].apply(matching_position_of_subset_items)
    possible_adresses_and_names["matches"] = possible_adresses_and_names.apply(
        lambda x: position_to_value(x["company_unique_id"], x["indices_mapping"]),
        axis=1,
    )

    # Transforms the pd.Series trading_as_companies["dictionary"] into one dictionary
    new_matches = {
        k: v for d in possible_adresses_and_names["matches"] for k, v in d.items()
    }

    return new_matches


def create_installer_unique_id(installer_data: pd.DataFrame):
    """
    Creates a string unique identifier for each installer company, since multiple company names might
    refer to the same installer.
    E.g. "Company A Ltd", "Company A Limited", "Company A (old account), "Company A t/a Company B"
    all refer to the same company which will be identified by the unique ID "company a".

    Identifiers are created from the original company names by:
    - cleaning the company name (lowe case, removing stopwords and punctuation);
    - Merge all different "trading as" expressions into one;
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

    # Droping processed_company_name as no longer needed
    installer_data.drop("processed_company_name", axis=1, inplace=True)

    # Check for those that have the same location but not identified as being the same
    same_postcode_no_match = installer_data.groupby("postcode", as_index=False)[
        ["company_unique_id"]
    ].nunique()
    same_postcode_no_match = same_postcode_no_match[
        same_postcode_no_match["company_unique_id"] > 1
    ]["postcode"].unique()
    new_matches = match_places_same_location_different_id(
        same_postcode_no_match, installer_data
    )
    installer_data["company_unique_id"] = installer_data["company_unique_id"].apply(
        lambda x: new_matches[x] if x in new_matches.keys() else x
    )


def geocode_postcode(data: pd.DataFrame, geodata: pd.DataFrame) -> pd.DataFrame:
    """
    Updates data with latitude and longitude columns, by merging with geodata
    on postode column. Also transforms postcode column by removing the space.

    Args:
        data: DataFrame with postcode column.
        geodata: DataFrame with postcode, latitude and longitude columns.
    """

    geodata["postcode"] = geodata["postcode"].str.replace(" ", "")

    data["postcode"] = data["postcode"].str.upper().str.replace(" ", "")
    data = data.merge(
        geodata[["postcode", "latitude", "longitude"]], how="left", on="postcode"
    )

    return data


def add_certification_body_info(installer_data, installations_data):
    """
    Adds certification body information to each instance of the historical installers table.

    Historical installers data does not come with certification body information, only with certification number.
    Hence, we can use the company name and the certification number to match to the installations table and get the
    certification body from there.


    """
    installations_match_vars = [
        "installation_company_name",
        "installation_company_mcs_number",
    ]
    installers_match_vars = ["company_name", "mcs_certificate_number"]

    # Unique pairs (installation_company_name, installation_company_mcs_number)
    unique_pairs_in_installations = installations_data.drop_duplicates(
        installations_match_vars
    )[installations_match_vars]

    # Certification number as float in both tables in order to be able to compare
    # installer_data["mcs_certificate_number"] = installer_data["mcs_certificate_number"].astype(float)

    # Merge installer with installation data to add the certification_body to the installer data
    installer_data = installer_data.merge(
        unique_pairs_in_installations,
        how="left",
        right_on=installations_match_vars,
        left_on=installers_match_vars,
    )

    installer_data.drop(installations_match_vars, axis=1, inplace=True)


def preprocess_historical_installers(
    raw_historical_installers,
    raw_historical_installations,
    geographical_data,
    companies_house_api_key,
):
    """
    Pre-processes raw historical installers data by:
    - Droping duplicate instances;
    - Renaming columns;
    - Joining installation and design variables;
    - Creating flag variables for whether an installer is certified for a certain technology;
    - Geocoding data by adding latitude and longitude;
    - Creating a variable that uniquely identifies installers;
    - Adding certification body information;

    Args:
        raw_historical_installers: raw historical installers data
        raw_historical_installations: raw historical installations data
        geographical_data: geographical data
        companies_house_api_key: companies house API key
    """

    # Apply basic preprocessing to table of installations
    basic_preprocessing_of_installations(raw_historical_installations)

    # Dropping duplicate lines
    raw_historical_installers.drop_duplicates(inplace=True)

    # Renaming columns
    raw_historical_installers.columns = rename_columns(
        raw_historical_installers.columns
    )

    # Joining installation and design variables
    joining_installation_and_design_vars(raw_historical_installers)

    # flag "original_records" (if originally in installers or inputed from installations)
    raw_historical_installers["original_record"] = [
        True for i in range(len(raw_historical_installers))
    ]

    # Get info installers with installations but with info missing from installers
    missing_installers = get_missing_installers_info(
        raw_historical_installations, raw_historical_installers, companies_house_api_key
    )

    raw_historical_installers = pd.concat(
        [raw_historical_installers, missing_installers]
    )
    raw_historical_installers.reset_index(drop=True, inplace=True)

    # Updating address_snippet variable
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

    # Certification body information
    add_certification_body_info(raw_historical_installers, raw_historical_installations)

    return raw_historical_installers[base_config.historical_installers_columns_order]


def create_argparser():
    parser = ArgumentParser()

    parser.add_argument(
        "--raw_historical_installers_filename",
        help="Raw historical installers file name",
        default="raw_historical_mcs_installers_20230207",
        type=str,
    )
    parser.add_argument(
        "--raw_historical_installations_filename",
        help="Raw historical installations file name",
        default="raw_historical_mcs_installers_20230207",
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
    bucket_name = base_config.BUCKET_NAME
    uk_geo_path = base_config.POSTCODE_TO_COORD_PATH

    uk_geo_data = load_s3_data(bucket_name, uk_geo_path)
    mcs_raw_data_path = base_config.S3_NEW_MCS_DATA_DUMP_DIR

    parser = create_argparser()
    args = parser.parse_args()

    raw_historical_installers = get_raw_historical_mcs_installers()

    raw_historical_installations = get_raw_historical_installations_data()

    processed_historical_installers = preprocess_historical_installers(
        raw_historical_installers,
        raw_historical_installations,
        uk_geo_data,
        args.api_key,
    )

    processed_data_path = base_config.PREPROCESSED_MCS_HISTORICAL_INSTALLERS_PATH

    # date when data was shared by MCS in MCS data dumps
    date = args.raw_historical_installations_filename.split("installers_")[1]

    # saving data to S3
    save_to_s3(
        s3,
        bucket_name,
        processed_historical_installers,
        processed_data_path.format(date),
    )
