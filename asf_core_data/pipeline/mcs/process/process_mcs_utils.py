# File: asf_core_data/pipeline/mcs/process/process_mcs_utils.py
"""
Functions used to preprocess mcs data across installations and installer company data.

Companies House API additional info:
- https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference/search/search-companies
- https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/resources/companysearch?v=latest

"""
# %%

import pandas as pd
import requests
import time
import re
import numpy as np
import random

# %%


def rename_columns(cols: list) -> list:
    """
    Renames a list of strings, e.g. a list of column names, by:
    - applying lower case;
    - replacing "heat pump" by "hp";
    - replacing " " and "/" by "_";
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


def clean_company_name(company_name: str) -> str:
    """
    Cleans installation company name by:
        - changing company name to lower case;
        - removing punctuation;
        - removing company-related stopwords like "ltd", "limited", etc.;

    Args:
        company_name: Name of company to clean.
    Returns:
        company_name: Cleaned company name.
    """
    company_stopwords = ["ltd", "limited", "old", "account"]

    company_name = company_name.lower().translate(str.maketrans("", "", "()-,/.&"))
    company_name = [
        comp for comp in company_name.split() if comp not in company_stopwords
    ]

    return " ".join(company_name)


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


def from_list_to_dictionary(list_values: list) -> dict:
    """
    Transforms a list into a dictionary where each key is matched to the first
    value in the list. Creates a mapping between each item in the list and the first one.
    Arg:
        list_values: a list of values e.g. ["Company A", "Company B", "Company A trading as Company B"]
    Returns:
        Dictionary mapping all values to the first one
        e.g. {"Company B":"Company A", "Company A trading as Company B": "Company A"}
    """
    return {value: list_values[0] for value in list_values[1:]}


def map_position_of_subset_items(list_of_strings: list) -> dict:
    """
    Returns a mapping between the position of strings in a list which are a subset of each other.
    Example:
    set_of_strings = ["company a ltd", "a ltd", "a"]
    Outputs {0: 2, 1: 2}
    Note that, in the example above, 0 is initially mapped to 1 but then that's overwritten by 0 being mapped to 2.

    Args:
        list_of_strings: a list of strings
    Returns:
        A dictionary mapping the position of strings that are subset of another.
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
        list_of_strings: list of strings, e.g. ["a", "b", "c"]
        indices_dict: dictionary of indices, e.g. {0:2, 1:2}
    Returns:
        The mapped dictionary e.g. {"a":"c", "b":"c"}

    """
    return {
        list_of_strings[key]: list_of_strings[indices_dict[key]]
        for key in indices_dict.keys()
    }


def match_companies_house(company_name: str, api_key: str) -> pd.Series:
    """
    Fuzzy match between MCS company name and Companies House API company names and
    returns Companies House information: company full address and company date of creation.

    Args:
        company_name: Company name used to query company house API
        api_key: API key to request data from live company house API endpoint
    Returns:
        company_info: pandas Series with 2 values, full address and creation date
    """

    # E.g. "Flo Group LTD T/A Flo Renewables" -> "flo renewables"
    company_name = company_name.lower()
    if "trading as" in company_name:
        company_name = deal_with_versions_of_trading_as(company_name).split(
            " trading as "
        )[1]

    # endpoint url
    base_url = f"https://api.company-information.service.gov.uk/search/companies?q={company_name}"

    response = requests.get(base_url, auth=(api_key, ""))

    # developer guidelines state you can make 600 requests per 5 mins
    # this translates into 1 request every 0.5 seconds
    time.sleep(0.5)

    response_status_code = response.status_code

    if response_status_code == 200:  # status code 200 means all good with request
        company_data = response.json()
        if (
            (company_data is not None)
            and ("items" in company_data.keys())
            and len(company_data["items"]) > 0
        ):
            address_snippet = (
                company_data["items"][0]["address_snippet"]
                if ("address_snippet" in company_data["items"][0].keys())
                else None
            )
            date_of_creation = (
                company_data["items"][0]["date_of_creation"]
                if "date_of_creation" in company_data["items"][0].keys()
                else None
            )

            return pd.Series([address_snippet, date_of_creation])

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


def geocode_postcode(data: pd.DataFrame, geodata: pd.DataFrame) -> pd.DataFrame:
    """
    Updates data with latitude and longitude columns, by merging with geodata
    on postode column (left merge, not to loose any data).
    Also transforms postcode column by removing the space.

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


def remove_punctuation(address):
    """
    Remove all unwanted punctuation from an address.
    Underscores are kept and slashes/dashes are converted
    to underscores so that the numeric tokens in e.g.
    "Flat 3/2" and "Flat 3-2" are treated as a whole later.

    Args:
        address (str): Address to format.

    Returns:
        address (str): Formatted address.
    """

    if (address is pd.NA) | (address is np.nan):
        return ""
    else:
        # Replace / and - with _
        address = re.sub(r"[/-]", "_", address)
        # Remove all punctuation other than _
        punct_regex = r"[\!\"#\$%&\\\'(\)\*\+,-\./:;<=>\?@\[\]\^`\{|\}~”“]"
        address = re.sub(punct_regex, "", address)

        return address


def extract_token_set(address, postcode, max_token_length):
    """
    Extract valid numeric tokens from address string.
    Numeric tokens are considered to be character strings containing numbers
    e.g. "45", "3a", "4_1".
    'Valid' is defined as
    - below a certain token_length (to remove long MPAN strings)
    - not the inward or outward code of the property's postcode
    - not the property's postcode with space removed

    Args:
        address (str): String from which to extract tokens.
        postcode (str): String used for removal of tokens corresponding to postcode parts.

    Returns:
        valid_token_set (set): Set of valid tokens. Set chosen as the order does not matter
        for comparison purposes.
    """
    # wonder if single-letter tokens should be in here too
    # for e.g. "Flat A" or whether this would give too many
    # false positives
    tokens = re.findall("\w*\d\w*", address)
    if pd.isnull(postcode):  # invalid postcode
        valid_tokens = [token for token in tokens if ((len(token) < max_token_length))]
    else:  # valid postcode
        valid_tokens = [
            token
            for token in tokens
            if (
                (len(token) < max_token_length)
                & (token.lower() not in postcode.lower().split())
                & (token.lower() != postcode.lower().replace(" ", ""))
            )
        ]
    valid_token_set = set(valid_tokens)

    return valid_token_set


## ----- Legacy functions and variables below -----
# The functions and variables below are legacy functions that are no longer in use.
# Some of them are commented because they still exist but have been adapter
colnames_dict = {
    "Version Number": "version",
    "Certificate Creation Date": "cert_date",
    "Commissioning Date": "commission_date",
    "Address Line 1": "address_1",
    "Address Line 2": "address_2",
    "Address Line 3": "address_3",
    "County": "county",
    "Postcode": "postcode",
    "Local Authority": "local_authority",
    "Total Installed Capacity": "capacity",
    "Estimated Annual Generation": "estimated_annual_generation",
    "Installation Company Name": "installer_name",
    "Green Deal Installation?": "green_deal",
    "Products": "products",
    "Flow temp/SCOP ": "flow_scop",
    "Technology Type": "tech_type",
    "Installation Type": "installation_type",
    " Installation Type": "installation_type",
    "End User Installation Type": "end_user_installation_type",
    "Installation New at Commissioning Date?": "new",
    "Renewable System Design": "design",
    "Annual Space Heating Demand": "heat_demand",
    "Annual Water Heating Demand": "water_demand",
    "Annual Space Heating Supplied": "heat_supplied",
    "Annual Water Heating Supplied": "water_supplied",
    "Installation Requires Metering?": "metering",
    "RHI Metering Status": "rhi_status",
    "RHI Metering Not Ready Reason": "rhi_not_ready",
    "Number of MCS Certificates": "n_certificates",
    "Heating System Type": "system_type",
    "Alternative Heating System Type": "alt_type",
    "Alternative Heating System Fuel Type": "alt_fuel",
    "Overall Cost": "cost",
    "Fuel Type": "fuel_type",
    "Installation Company MCS Number": "installation_company_mcs_number",  # new var provided now in installations
}

mcs_companies_dict = {
    "Company Name": "installer_name",
    "MCS certificate number": "certificate_number",
    "Add 1": "address_1",
    "Add2": "address_2",
    "Town": "town",
    "County": "county",
    "PCode": "postcode",
    "Solar Thermal": "solar_thermal",
    "Wind Turbines": "wind_turbines",
    "Air Source Heat Pumps": "air_source_hps",
    "Exhaust Air Heat Pumps": "exhaust_air_hps",
    "Biomass": "biomass",
    "Solar Photovoltaics": "solar_pv",
    "Micro CHP": "micro_chp",
    "Solar Assisted Heat Pump": "solar_assisted_hps",
    "Gas Absorption Heat Pump": "gas_absorption_hps",
    "Ground/Water Source Heat Pump": "ground_water_hps",
    "Battery Storage": "battery_storage",
    "Eastern Region": "eastern_region",
    "East Midlands Region": "east_midlands_region",
    "London Region": "london_region",
    "North East Region": "north_east_region",
    "North West Region": "north_west_region",
    "South East Region": "south_east_region",
    "South West Region": "south_west_region",
    "West Midlands Region": "west_midlands_region",
    "Yorkshire Humberside Region": "yorkshire_humberside_region",
    "Northern Ireland Region": "northern_ireland_region",
    "Scotland Region": "scotland_region",
    "Wales Region": "wales_region",
    "Effective From": "effective_from",
    "Consumer Code": "consumer_code",
    "Certification Body": "certification_body",
}
"""
def clean_concat_installers(data):

    Legacy function: only used for quarterly installers data,
    not for historical installers.

    Cleans concatenated installers data by:
        - dropping duplicate company names;
        - removing columns with identical values;
        - merging similar columns.

    Args:
        data (pd.DataFrame): Concatenated installers data

    Returns:
        data (pd.DataFrame): Cleaned, concatenated installers data



    keywords_to_merge = [
        "Air Source",
        "Exhaust Air",
        "Assisted",
        "Absorption",
        "Ground/Water",
    ]
    # deduplicate
    data = data.drop_duplicates(subset="Company Name")
    # installation and design is the same; drop columns
    data = data[[col for col in data.columns if "Design" not in col]]
    # merge columns iteratively
    for keyword in keywords_to_merge:
        hp_types = data[[col for col in data.columns if keyword in col]]
        data[hp_types.columns[0]] = (
            hp_types.where(hp_types.ne(0), np.nan)
            .bfill(axis=1)[hp_types.columns[0]]
            .fillna(0)
        )
        data.drop(columns=hp_types.columns[1], axis=1, inplace=True)

    data.columns = [
        " ".join(c.split(" ")[:-1]) if "Installation" in c else c for c in data.columns
    ]

    return data.reset_index(drop=True)
"""

# The functions and variables below were originally created by India and used to process the installers data we were receving
# from MCS each quarter. Now that we are working with the historical table of installers, we had to change these
# functions slighly as per above. We kept the original ones for reference, if we ever need to go back to them to
# repeat any analysis.
#
# def clean_company_name(company_name):
#    """cleans installation company name by:
#        - removing "company" stopwords like ltd, limited;
#        - lowers company name
#        - removes punctuation
#    Args:
#        company_name (str): Name of company to clean.
#    Returns:
#        company_name (str): Cleaned company name.
#    """
#    company_stopwords = ["ltd", "ltd.", "limited", "limited.", "old", "account", "t/a"]
#
#    company_name = company_name.lower().translate(str.maketrans("", "", "()-"))
#    company_name = [
#        comp for comp in company_name.split() if comp not in company_stopwords
#    ]
#
#    return " ".join(company_name)
#
#
# def geocode_postcode(data, geodata):
#    """geocodes dataset with 'postcode' column.
#    Args:
#        data (pd.DataFrame): DataFrame with 'postcode' column.
#        geodata (pd.DataFrame): DataFrame with 'postcode', 'latitude' and 'longitude' columns.
#    Returns:
#        data_with_geodata (pd.DataFrame): Merged data DataFrame with 'latitude' and 'longitude'.
#    """
#
#    geodata["postcode"] = geodata["postcode"].str.replace(" ", "")
#
#    if "postcode" in data.columns:
#        data["postcode"] = data["postcode"].str.upper().str.replace(" ", "")
#        data_with_postcode = pd.merge(
#            data, geodata[["postcode", "latitude", "longitude"]], on="postcode"
#        )
#        print(
#            f"lost {len(list(set(data.postcode) - set(geodata.postcode)))} postcodes in merge."
#        )
#        return data_with_postcode
#    else:
#        print("'postcode' not found in data columns.")
#
#
# def match_companies_house(company_name, api_key):
#    """Fuzzy matches company name with company house API and
#        returns companies house information.
#    Args:
#        company_name (str): Company name to query company house API with.
#        api_key (str): Api key to call live company house API end point.
#    Returns:
#        company_info (dict): Dictionary of company house data associated to company name.
#    """
#
#    if len(company_name.split()) == 1:
#        company_name
#    else:
#        company_name = "+".join(company_name.split())
#
#    company_info = dict()
#    base_url = "https://api.company-information.service.gov.uk/search/companies?q={company_name}".format(
#        company_name=company_name
#    )
#    response = requests.get(base_url, auth=(api_key, ""))
#
#    if response.status_code == 200:
#        company_data = response.json()
#        if company_data is not None:
#            if "items" in company_data.keys():
#                if company_data["items"] != []:
#                    closest_match = company_data["items"][
#                        0
#                    ]  # closest match according to companies house API
#
#                    company_info[company_name.replace("+", " ")] = {
#                        "company_number": closest_match["company_number"],
#                        "title": closest_match["title"],
#                        "company_status": closest_match["company_status"],
#                        "description_identifier": closest_match[
#                            "description_identifier"
#                        ],
#                        "address_snippet": closest_match["address_snippet"],
#                    }
#        return company_info
#
#    elif response.status_code == 429:
#        print(f"{response.status_code} error! Sleeping for 5 mins...")
#        time.sleep(
#            301
#        )  # sleep for 5 minutes as developer guidelines state you can make 600 requests per 5 mins
#    else:
#        print(f"{response.status_code} error!")
