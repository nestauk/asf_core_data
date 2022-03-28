# File: asf_core_data/pipeline/process_mcs_utils.py
"""
functions used to preprocess mcs data across installations and
installer company data.
"""
#######################################################

import pandas as pd
import requests
import time
import string

#######################################################

colnames_dict = {
    "Version Number": "version",
    "Certificate Creation Date": "cert_date",
    "Commissioning Date": "date",
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
    "Heating System Type": "system_type",  # check - what's this?
    "Alternative Heating System Type": "alt_type",
    "Alternative Heating System Fuel Type": "alt_fuel",
    "Overall Cost": "cost",
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
    "Exhaust Air Heat Pumps": "exhaust_air_hps",
    "Biomass": "biomass",
    "Solar Photovoltaics": "solar_pv",
    "Micro CHP": "micro_chp",
    "SolarAssistedHeatPump": "solar_assisted_hps",
    "GasAbsorptionHeatPump": "gas_absorption_hps",
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


def clean_company_name(company_name):
    """cleans installation company name by:
        - removing "company" stopwords like ltd, limited;
        - lowers company name
        - removes punctuation
    Args:
        company_name (str): Name of company to clean.
    Returns:
        company_name (str): Cleaned company name.
    """
    company_stopwords = ["ltd", "ltd.", "limited", "limited.", "old", "account", "t/a"]

    company_name = company_name.lower().translate(str.maketrans("", "", "()-"))
    company_name = [
        comp for comp in company_name.split() if comp not in company_stopwords
    ]

    return " ".join(company_name)


def geocode_postcode(data, geodata):
    """geocodes dataset with 'postcode' column.
    Args:
        data (pd.DataFrame): DataFrame with 'postcode' column.
        geodata (pd.DataFrame): DataFrame with 'postcode', 'latitude' and 'longitude' columns.
    Returns:
        data_with_geodata (pd.DataFrame): Merged data DataFrame with 'latitude' and 'longitude'.
    """

    geodata["postcode"] = geodata["postcode"].str.replace(" ", "")

    if "postcode" in data.columns:
        data["postcode"] = data["postcode"].str.upper().str.replace(" ", "")
        data_with_postcode = pd.merge(
            data, geodata[["postcode", "latitude", "longitude"]], on="postcode"
        )
        print(
            f"lost {len(list(set(data.postcode) - set(geodata.postcode)))} postcodes in merge."
        )
        return data_with_postcode
    else:
        print("'postcode' not found in data columns.")


def match_companies_house(company_name, api_key):
    """Fuzzy matches company name with company house API and
        returns companies house information.
    Args:
        company_name (str): Company name to query company house API with.
        api_key (str): Api key to call live company house API end point.
    Returns:
        company_info (dict): Dictionary of company house data associated to company name.
    """

    if len(company_name.split()) == 1:
        company_name
    else:
        company_name = "+".join(company_name.split())

    company_info = dict()
    base_url = "https://api.company-information.service.gov.uk/search/companies?q={company_name}".format(
        company_name=company_name
    )
    response = requests.get(base_url, auth=(api_key, ""))

    if response.status_code == 200:
        company_data = response.json()
        if company_data is not None:
            if "items" in company_data.keys():
                if company_data["items"] != []:
                    closest_match = company_data["items"][
                        0
                    ]  # closest match according to companies house API

                    company_info[company_name.replace("+", " ")] = {
                        "company_number": closest_match["company_number"],
                        "title": closest_match["title"],
                        "company_status": closest_match["company_status"],
                        "description_identifier": closest_match[
                            "description_identifier"
                        ],
                        "address_snippet": closest_match["address_snippet"],
                    }
        return company_info

    elif response.status_code == 429:
        print(f"{response.status_code} error! Sleeping for 5 mins...")
        time.sleep(
            301
        )  # sleep for 5 minutes as developer guidelines state you can make 600 requests per 5 mins
    else:
        print(f"{response.status_code} error!")
