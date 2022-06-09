# %%
# File: asf_core_data/pipeline/process_mcs_installers.py
"""Processing MCS installer company data.

To run script, (in activated conda environment) python process_mcs_installers.py -key API KEY
"""
##########################################################
import argparse
import pandas as pd

from asf_core_data.pipeline.mcs.process.process_mcs_installations import (
    get_processed_installations_data,
)

from asf_core_data.getters.data_getters import s3, load_s3_data, save_to_s3

from asf_core_data.pipeline.mcs.process.process_mcs_utils import (
    mcs_companies_dict,
    clean_company_name,
    geocode_postcode,
    match_companies_house,
    clean_concat_installers,
)

from asf_core_data import bucket_name, get_yaml_config, _base_config_path

##########################################################


def preprocess_installer_companies(installer_companies, installations_data, api_key):
    """Preprocesses installer companies data by:
        - Renaming columns
        - Dropping 'unspecified' company values
        - Cleaning company names
        - Fuzzy matching company names w/o addresses to Companies House API
    Args:
        installer_companies (pd.DataFrame): Loaded, raw MCS installer company data.
        installations_data (pd.DataFrame): Cleaned, domestic MCS installations data.
        api_key (str): API key needed to call live Companies House API endpoint.
    Returns:
        installer_companies_house_data (pd.DataFrame):
            Cleaned, enriched installer companies data.
    """

    # rename columns
    installer_companies = installer_companies.rename(columns=mcs_companies_dict)

    # make sure they're heat pump companies
    hp_cols = [col for col in installer_companies.columns if "hps" in col]
    installer_companies = installer_companies[
        installer_companies[hp_cols].eq("YES").any(axis=1)
    ]

    # drop unspecified column
    installer_companies = installer_companies[
        installer_companies["installer_name"] != "Unspecified"
    ]
    # clean up company name
    installer_companies["installer_name"] = installer_companies["installer_name"].apply(
        clean_company_name
    )

    # get a list of companies NOT installer company data
    missing_installer_companies = list(
        set(installations_data.installer_name) - set(installer_companies.installer_name)
    )

    # fuzzy match installer companies NOT in installer company data to companies house API
    companies_house_data = [
        match_companies_house(comp, api_key) for comp in missing_installer_companies
    ]

    # turn to dataframe
    companies_house_df = pd.concat(
        [pd.DataFrame(l) for l in companies_house_data], axis=1
    ).T.reset_index()
    companies_house_df = companies_house_df.rename(columns={"index": "installer_name"})

    # df if address snippet not none
    companies_house_df = companies_house_df[
        ~companies_house_df["address_snippet"].isna()
    ]

    # extract address information
    companies_house_df["address_1"] = companies_house_df["address_snippet"].apply(
        lambda x: x.split(",")[0]
    )
    companies_house_df["postcode"] = companies_house_df["address_snippet"].apply(
        lambda x: x.split(",")[-1].strip()
    )
    # keep specific columns
    companies_house_df = companies_house_df[
        [
            "company_status",
            "installer_name",
            "address_1",
            "postcode",
            "address_snippet",
        ]
    ]

    # merged companies house data w/ installer companies data
    installer_companies_house_data = pd.concat(
        [installer_companies, companies_house_df]
    )

    return installer_companies_house_data


if __name__ == "__main__":
    # get config file with relevant paramenters
    # get config file with relevant paramenters
    config_info = get_yaml_config(_base_config_path)
    installer_company_data_path = config_info["MCS_RAW_INSTALLER_CONCAT_S3_PATH"]
    uk_geo_path = config_info["UK_GEO_PATH"]
    cleaned_installations_path = config_info["PREPROC_GEO_MCS_INSTALLATIONS_PATH"]
    cleaned_installer_company_path = config_info["PREPROC_MCS_INSTALLER_COMPANY_PATH"]

    # pass API key when script is run
    parser = argparse.ArgumentParser()
    parser.add_argument("-key", "--api_key", help="live Companies House API key")
    args = parser.parse_args()
    api_key = args.api_key

    # LOAD DATA
    ## load data from s3
    installer_company_data = load_s3_data(bucket_name, installer_company_data_path)
    uk_geo_data = load_s3_data(bucket_name, uk_geo_path)

    ## load cleaned installations data
    mcs_data = get_processed_installations_data(refresh=True)

    ## preprocess different columns
    installer_company_data = clean_concat_installers(installer_company_data)

    # PREPROCESS INSTALLER COMPANY DATA
    cleaned_installers_data = preprocess_installer_companies(
        installer_company_data, mcs_data, api_key
    )

    cleaned_installers_data = cleaned_installers_data.drop_duplicates(
        subset=["address_1", "postcode"]
    )

    # geocode data
    geocoded_cleaned_installations_data = geocode_postcode(mcs_data, uk_geo_data)
    geocoded_cleaned_installers_data = geocode_postcode(
        cleaned_installers_data, uk_geo_data
    )

    # save preprocessed, geocoded data to s3
    save_to_s3(
        s3, bucket_name, geocoded_cleaned_installations_data, cleaned_installations_path
    )
    save_to_s3(
        s3,
        bucket_name,
        geocoded_cleaned_installers_data,
        cleaned_installer_company_path,
    )
