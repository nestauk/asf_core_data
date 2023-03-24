from asf_core_data import Path
from datetime import datetime

MCS_RAW_S3_PATH = "inputs/MCS/mcs_heat_pumps.xlsx"
MCS_RAW_LOCAL_PATH = "/inputs/data/mcs/mcs_heat_pumps.xlsx"

CURRENT_YEAR = 2022

ROOT_DATA_PATH = "."
BUCKET_NAME = "asf-core-data"

# Cleaning settings
MERGED_AGE_BANDS = True
GLAZED_AREA_AS_NUM = True
FLOOR_LEVEL_AS_NUM = True
ONLY_FIRST_EFF = False

GLAZED_AREA_DTYPE = float if GLAZED_AREA_AS_NUM else str
FLOOR_LEVEL_DTYPE = float if FLOOR_LEVEL_AS_NUM else str

v0_batches = [
    "2021_Q2_0721",
]

POSTCODE_TO_COORD_PATH = Path(
    "inputs/supplementary_data/geospatial/ukpostcodes_to_coordindates.csv"
)

SUPPL_DATA_PATH = "inputs/supplementary_data/"

EST_CLEANSED_EPC_DATA_DEDUPL_PATH = Path(
    "inputs/EPC/EST_cleansed_versions/EPC_England_Wales_cleansed_and_deduplicated.csv"
)
EST_CLEANSED_EPC_DATA_DEDUPL_PATH_ZIP = Path(
    "inputs/EPC/EST_cleansed_versions/EPC_England_Wales_cleansed_and_deduplicated.csv.zip"
)

EST_CLEANSED_EPC_DATA_PATH = Path(
    "inputs/EPC/EST_cleansed_versions/EPC_England_Wales_cleansed.csv"
)
EST_CLEANSED_EPC_DATA_PATH_ZIP = Path(
    "inputs/EPC/EST_cleansed_versions/EPC_England_Wales_cleansed.csv.zip"
)

RAW_EPC_DATA_PATH = Path("outputs/EPC/preprocessed_data/{}/EPC_GB_raw.csv")

PREPROC_EPC_DATA_DEDUPL_PATH = Path(
    "outputs/EPC/preprocessed_data/{}/EPC_GB_preprocessed_and_deduplicated.csv"
)

OUTPUT_DATA_PATH = Path("outputs/EPC/preprocessed_data/")

PREPROC_EPC_DATA_PATH = Path("outputs/EPC/preprocessed_data/{}/EPC_GB_preprocessed.csv")

PROCESSED_EPC_DATA_PATH = Path("./outputs/EPC/preprocessed_data/{}/")


RAW_ENG_WALES_DATA_ZIP = Path(
    "inputs/EPC/raw_data/{}/England_Wales/all-domestic-certificates.zip"
)
RAW_SCOTLAND_DATA_ZIP = Path("inputs/EPC/raw_data/{}/Scotland/D_EPC_data.zip")

RAW_DATA_PATH = Path("inputs/EPC/raw_data/{}/")

RAW_DATA_FILE = Path("inputs/EPC/raw_data/{}")

RAW_ENG_WALES_DATA_PATH = Path("inputs/EPC/raw_data/{}/England_Wales/")
RAW_SCOTLAND_DATA_PATH = Path("inputs/EPC/raw_data/{}/Scotland/")

IMD_PATH = Path("inputs/supplementary_data/IMD/")
IMD_ENGLAND_PATH = Path("inputs/supplementary_data/IMD/England_IMD.csv")
IMD_WALES_PATH = Path("inputs/supplementary_data/IMD/Wales_IMD.csv")
IMD_SCOTLAND_PATH = Path("inputs/supplementary_data/IMD/Scotland_IMD.csv")

POSTCODE_TO_COORD_PATH = Path(
    "inputs/supplementary_data/geospatial/ukpostcodes_to_coordindates.csv"
)
MERGED_MCS_EPC = Path("inputs/MCS_data/mcs_epc.csv")

SUPERVISED_MODEL_OUTPUT = Path("outputs/supervised_model/")
SUPERVISED_MODEL_FIG_PATH = Path("outputs/supervised_model/figures/")
HEAT_PUMP_COSTS_FIG_PATH = Path("outputs/figures/hp_costs/")

FIG_PATH = Path("outputs/figures")

KEPLER_OUTPUT = Path("outputs/figures/kepler/")

EPC_FEAT_SELECTION = [
    "ADDRESS1",
    "ADDRESS2",
    "POSTCODE",
    "MAINS_GAS_FLAG",
    "NUMBER_HABITABLE_ROOMS",
    "CURRENT_ENERGY_RATING",
    "POTENTIAL_ENERGY_RATING",
    "CURRENT_ENERGY_EFFICIENCY",
    "ENERGY_CONSUMPTION_CURRENT",
    "TENURE",
    "MAINHEAT_ENERGY_EFF",
    "HOT_WATER_ENERGY_EFF",
    "FLOOR_ENERGY_EFF",
    "WINDOWS_ENERGY_EFF",
    "WALLS_ENERGY_EFF",
    "ROOF_ENERGY_EFF",
    "MAINHEATC_ENERGY_EFF",
    "LIGHTING_ENERGY_EFF",
    "MAINHEAT_DESCRIPTION",
    "CO2_EMISSIONS_CURRENT",
    "CO2_EMISS_CURR_PER_FLOOR_AREA",
    "BUILDING_REFERENCE_NUMBER",
    "INSPECTION_DATE",
    "BUILT_FORM",
    "PROPERTY_TYPE",
    "CONSTRUCTION_AGE_BAND",
    "TRANSACTION_TYPE",
    "TOTAL_FLOOR_AREA",
    "ENERGY_TARIFF",
    "UPRN",
    "SECONDHEAT_DESCRIPTION",
    "FLOOR_LEVEL",
    "LOCAL_AUTHORITY",
    "LOCAL_AUTHORITY_LABEL",
    "GLAZED_AREA",
    "GLAZED_TYPE",
    "PHOTO_SUPPLY",
    "NUMBER_HEATED_ROOMS",
    "SOLAR_WATER_HEATING_FLAG",
    "LMK_KEY",
    "WINDOWS_DESCRIPTION",
    "HOTWATER_DESCRIPTION",
    "FLOOR_DESCRIPTION",
    "WALLS_DESCRIPTION",
    "ROOF_DESCRIPTION",
    "LIGHTING_DESCRIPTION",
    "MAIN_HEATING_CONTROLS",
    "MULTI_GLAZE_PROPORTION",
    "LOW_ENERGY_LIGHTING",
    "COUNTRY",
]


EPC_PREPROC_FEAT_SELECTION = EPC_FEAT_SELECTION + [
    "N_SAME_UPRN_ENTRIES",
    "HEATING_SYSTEM",
    "HEATING_FUEL",
    "HP_INSTALLED",
    "HP_TYPE",
    "CURR_ENERGY_RATING_NUM",
    "ENERGY_RATING_CAT",
    "DIFF_POT_ENERGY_RATING",
]


dtypes = {
    # EPC features
    "LMK_KEY": str,
    "ADDRESS1": str,
    "ADDRESS2": str,
    "ADDRESS3": str,
    "POSTCODE": str,
    "BUILDING_REFERENCE_NUMBER": str,
    "CURRENT_ENERGY_RATING": str,
    "POTENTIAL_ENERGY_RATING": str,
    "CURRENT_ENERGY_EFFICIENCY": int,
    "POTENTIAL_ENERGY_EFFICIENCY": int,
    "PROPERTY_TYPE": str,
    "BUILT_FORM": str,
    "INSPECTION_DATE": str,
    "LOCAL_AUTHORITY": str,
    "CONSTITUENCY": str,
    "COUNTY": str,
    "LODGEMENT_DATE": str,
    "TRANSACTION_TYPE": str,
    "ENVIRONMENT_IMPACT_CURRENT": int,
    "ENVIRONMENT_IMPACT_POTENTIAL": int,
    "ENERGY_CONSUMPTION_CURRENT": float,
    "ENERGY_CONSUMPTION_POTENTIAL": float,
    "CO2_EMISSIONS_CURRENT": float,
    "CO2_EMISSIONS_POTENTIAL": float,
    "CO2_EMISS_CURR_PER_FLOOR_AREA": float,
    "LIGHTING_COST_CURRENT": float,
    "LIGHTING_COST_POTENTIAL": float,
    "HEATING_COST_CURRENT": float,
    "HEATING_COST_POTENTIAL": float,
    "HOT_WATER_COST_CURRENT": float,
    "HOT_WATER_COST_POTENTIAL": float,
    "TOTAL_FLOOR_AREA": float,
    "ENERGY_TARIFF": str,
    "MAINS_GAS_FLAG": str,
    "FLOOR_LEVEL": str,
    "FLAT_TOP_STOREY": str,
    "FLAT_STOREY_COUNT": float,
    "MAIN_HEATING_CONTROLS": str,
    "MULTI_GLAZE_PROPORTION": float,
    "GLAZED_TYPE": str,
    "GLAZED_AREA": str,
    "EXTENSION_COUNT": float,
    "MECHANICAL_VENTILATION": str,
    "NUMBER_HABITABLE_ROOMS": float,
    "NUMBER_HEATED_ROOMS": float,
    "LOW_ENERGY_LIGHTING": float,
    "NUMBER_OPEN_FIREPLACES": float,
    "HOTWATER_DESCRIPTION": str,
    "HOT_WATER_ENERGY_EFF": str,
    "HOT_WATER_ENV_EFF": str,
    "FLOOR_DESCRIPTION": str,
    "FLOOR_ENERGY_EFF": str,
    "FLOOR_ENV_EFF": str,
    "WINDOWS_DESCRIPTION": str,
    "WINDOWS_ENERGY_EFF": str,
    "WINDOWS_ENV_EFF": str,
    "WALLS_DESCRIPTION": str,
    "WALLS_ENERGY_EFF": str,
    "WALLS_ENV_EFF": str,
    "SECONDHEAT_DESCRIPTION": str,
    "SHEATING_ENERGY_EFF": str,
    "SHEATING_ENV_EFF": str,
    "ROOF_DESCRIPTION": str,
    "ROOF_ENERGY_EFF": str,
    "ROOF_ENV_EFF": str,
    "MAINHEAT_DESCRIPTION": str,
    "MAINHEAT_ENERGY_EFF": str,
    "MAINHEAT_ENV_EFF": str,
    "MAINHEATCONT_DESCRIPTION": str,
    "MAINHEATC_ENERGY_EFF": str,
    "MAINHEATC_ENV_EFF": str,
    "LIGHTING_DESCRIPTION": str,
    "LIGHTING_ENERGY_EFF": str,
    "LIGHTING_ENV_EFF": str,
    "MAIN_FUEL": str,
    "WIND_TURBINE_COUNT": float,
    "HEAT_LOSS_CORRIDOR": str,
    "UNHEATED_CORRIDOR_LENGTH": float,
    "FLOOR_HEIGHT": float,
    "PHOTO_SUPPLY": str,
    "SOLAR_WATER_HEATING_FLAG": str,
    "MECHANICAL_VENTILATION": str,
    "ADDRESS": str,
    "LOCAL_AUTHORITY_LABEL": str,
    "CONSTITUENCY_LABEL": str,
    "POSTTOWN": str,
    "CONSTRUCTION_AGE_BAND": str,
    "LODGEMENT_DATE": str,
    "LODGEMENT_DATE_TIME": str,
    "TENURE": str,
    "FIXED_LIGHTING_OUTLETS_COUNT": float,
    "LOW_ENERGY_FIXED_LIGHT_COUNT": float,
    "UPRN": str,
    "UPRN_SOURCE": str,
    "LODGEMENT_DATETIME": str,
    "COUNTRY": str,
    # Scotland features
    "OSG_REFERENCE_NUMBER": str,
    "TYPE_OF_ASSESSMENT": str,
    "3_YR_ENERGY_COST_CURRENT": float,
    "3_YR_ENERGY_SAVING_POTENTIAL": float,
    "CURRENT_ENVIRONMENTAL_RATING": str,
    "POTENTIAL_ENVIRONMENTAL_RATING": str,
    "IMPROVEMENTS": str,
    "ALTERNATIVE_IMPROVEMENTS": str,
    "AIR_TIGHTNESS_DESCRIPTION": str,
    "AIR_TIGHTNESS_ENERGY_EFF": str,
    "AIR_TIGHTNESS_ENV_EFF": str,
    "LZC_ENERGY_SOURCES": str,
    "IMPACT_LOFT_INSULATION": float,
    "IMPACT_CAVITY_WALL_INSULATION": float,
    "IMPACT_SOLID_WALL_INSULATION": float,
    "ADDENDUM_TEXT": str,
    "DATA_ZONE": str,
    "WALL_DESCRIPTION": str,
    "SPACE_HEATING_DEMAND": float,
    "WATER_HEATING_DEMAND": float,
    "HEAT_LOSS_CORRIDOOR": str,
    "MAIN_HEATING_CATEGORY": str,
    "Flat Location": str,
}

dtypes_prepr = {
    # EPC features
    "LMK_KEY": str,
    "ADDRESS1": str,
    "ADDRESS2": str,
    "ADDRESS3": str,
    "POSTCODE": str,
    "BUILDING_REFERENCE_NUMBER": str,
    "CURRENT_ENERGY_RATING": str,
    "POTENTIAL_ENERGY_RATING": str,
    "CURRENT_ENERGY_EFFICIENCY": int,
    "POTENTIAL_ENERGY_EFFICIENCY": int,
    "PROPERTY_TYPE": str,
    "BUILT_FORM": str,
    "INSPECTION_DATE": str,
    "LOCAL_AUTHORITY": str,
    "CONSTITUENCY": str,
    "COUNTY": str,
    "LODGEMENT_DATE": str,
    "TRANSACTION_TYPE": str,
    "ENVIRONMENT_IMPACT_CURRENT": int,
    "ENVIRONMENT_IMPACT_POTENTIAL": int,
    "ENERGY_CONSUMPTION_CURRENT": float,
    "ENERGY_CONSUMPTION_POTENTIAL": float,
    "CO2_EMISSIONS_CURRENT": float,
    "CO2_EMISSIONS_POTENTIAL": float,
    "CO2_EMISS_CURR_PER_FLOOR_AREA": float,
    "LIGHTING_COST_CURRENT": float,
    "LIGHTING_COST_POTENTIAL": float,
    "HEATING_COST_CURRENT": float,
    "HEATING_COST_POTENTIAL": float,
    "HOT_WATER_COST_CURRENT": float,
    "HOT_WATER_COST_POTENTIAL": float,
    "TOTAL_FLOOR_AREA": float,
    "ENERGY_TARIFF": str,
    "MAINS_GAS_FLAG": str,
    "FLOOR_LEVEL": FLOOR_LEVEL_DTYPE,
    "FLAT_TOP_STOREY": str,
    "FLAT_STOREY_COUNT": float,
    "MAIN_HEATING_CONTROLS": str,
    "MULTI_GLAZE_PROPORTION": float,
    "GLAZED_TYPE": str,
    "GLAZED_AREA": GLAZED_AREA_DTYPE,
    "EXTENSION_COUNT": float,
    "MECHANICAL_VENTILATION": str,
    "NUMBER_HABITABLE_ROOMS": float,
    "NUMBER_HEATED_ROOMS": float,
    "LOW_ENERGY_LIGHTING": float,
    "NUMBER_OPEN_FIREPLACES": float,
    "HOTWATER_DESCRIPTION": str,
    "HOT_WATER_ENERGY_EFF": str,
    "HOT_WATER_ENV_EFF": str,
    "FLOOR_DESCRIPTION": str,
    "FLOOR_ENERGY_EFF": str,
    "FLOOR_ENV_EFF": str,
    "WINDOWS_DESCRIPTION": str,
    "WINDOWS_ENERGY_EFF": str,
    "WINDOWS_ENV_EFF": str,
    "WALLS_DESCRIPTION": str,
    "WALLS_ENERGY_EFF": str,
    "WALLS_ENV_EFF": str,
    "SECONDHEAT_DESCRIPTION": str,
    "SHEATING_ENERGY_EFF": str,
    "SHEATING_ENV_EFF": str,
    "ROOF_DESCRIPTION": str,
    "ROOF_ENERGY_EFF": str,
    "ROOF_ENV_EFF": str,
    "MAINHEAT_DESCRIPTION": str,
    "MAINHEAT_ENERGY_EFF": str,
    "MAINHEAT_ENV_EFF": str,
    "MAINHEATCONT_DESCRIPTION": str,
    "MAINHEATC_ENERGY_EFF": str,
    "MAINHEATC_ENV_EFF": str,
    "LIGHTING_DESCRIPTION": str,
    "LIGHTING_ENERGY_EFF": str,
    "LIGHTING_ENV_EFF": str,
    "MAIN_FUEL": str,
    "WIND_TURBINE_COUNT": float,
    "HEAT_LOSS_CORRIDOR": str,
    "UNHEATED_CORRIDOR_LENGTH": float,
    "FLOOR_HEIGHT": float,
    "PHOTO_SUPPLY": float,
    "SOLAR_WATER_HEATING_FLAG": str,
    "MECHANICAL_VENTILATION": str,
    "ADDRESS": str,
    "LOCAL_AUTHORITY_LABEL": str,
    "CONSTITUENCY_LABEL": str,
    "POSTTOWN": str,
    "CONSTRUCTION_AGE_BAND": str,
    "LODGEMENT_DATE": str,
    "LODGEMENT_DATE_TIME": str,
    "TENURE": str,
    "FIXED_LIGHTING_OUTLETS_COUNT": float,
    "LOW_ENERGY_FIXED_LIGHT_COUNT": float,
    "UPRN": str,
    "UPRN_SOURCE": str,
    "LODGEMENT_DATETIME": str,
    # new features
    "COUNTRY": str,
    "N_ENTRIES": int,
    "HEATING_SYSTEM": str,
    "HEATING_FUEL": str,
    "HP_INSTALLED": bool,
    "HP_TYPE": str,
    "ENERGY_RATING_CAT": str,
    "DIFF_POT_ENERGY_RATING": float,
    "WALLS_ENERGY_EFF_SCORE": float,
    "WALLS_ENV_EFF_SCORE": float,
    "ROOF_ENERGY_EFF_SCORE": float,
    "ROOF_ENV_EFF_SCORE": float,
    "FLOOR_ENERGY_EFF_SCORE": float,
    "FLOOR_ENV_EFF_SCORE": float,
    "WINDOWS_ENERGY_EFF_SCORE": float,
    "WINDOWS_ENV_EFF_SCORE": float,
    "MAINHEAT_ENERGY_EFF_SCORE": float,
    "MAINHEAT_ENV_EFF_SCORE": float,
    "MAINHEATC_ENERGY_EFF_SCORE": float,
    "MAINHEATC_ENV_EFF_SCORE": float,
    "HOT_WATER_ENERGY_EFF_SCORE": float,
    "HOT_WATER_ENV_EFF_SCORE": float,
    "LIGHTING_ENERGY_EFF_SCORE": float,
    "LIGHTING_ENV_EFF_SCORE": float,
    "AIR_TIGHTNESS_ENERGY_EFF_SCORE": float,
    "AIR_TIGHTNESS_ENV_EFF_SCORE": float,
    # Scotland features
    "OSG_REFERENCE_NUMBER": str,
    "TYPE_OF_ASSESSMENT": str,
    "3_YR_ENERGY_COST_CURRENT": float,
    "3_YR_ENERGY_SAVING_POTENTIAL": float,
    "CURRENT_ENVIRONMENTAL_RATING": str,
    "POTENTIAL_ENVIRONMENTAL_RATING": str,
    "IMPROVEMENTS": str,
    "ALTERNATIVE_IMPROVEMENTS": str,
    "AIR_TIGHTNESS_DESCRIPTION": str,
    "AIR_TIGHTNESS_ENERGY_EFF": str,
    "AIR_TIGHTNESS_ENV_EFF": str,
    "LZC_ENERGY_SOURCES": str,
    "IMPACT_LOFT_INSULATION": float,
    "IMPACT_CAVITY_WALL_INSULATION": float,
    "IMPACT_SOLID_WALL_INSULATION": float,
    "ADDENDUM_TEXT": str,
    "DATA_ZONE": str,
    "WALL_DESCRIPTION": str,
    "SPACE_HEATING_DEMAND": float,
    "WATER_HEATING_DEMAND": float,
    "HEAT_LOSS_CORRIDOR": str,
    "MAIN_HEATING_CATEGORY": str,
    "Flat Location": str,
}


dtypes_recom = {
    "LMK_KEY": str,
    "IMPROVEMENT_ITEM": str,
    "IMPROVEMENT_SUMMARY_TEXT": str,
    "IMPROVEMENT_DESCR_TEXT": str,
    "IMPROVEMENT_ID": str,
    "IMPROVEMENT_ID_TEXT": str,
    "INDICATIVE_COST": str,
}

MCS_HP_PATH = Path("/inputs/MCS_data/mcs_heat_pumps.xlsx")
MCS_DOMESTIC_HP_PATH = Path("/outputs/mcs_domestic_hps.csv")
INFLATION_PATH = Path("/inputs/MCS_data/inflation.csv")
MCS_EPC_MERGED_PATH = Path("outputs/MCS/mcs_epc.csv")

parse_dates = [
    "INSPECTION_DATE",
    "LODGEMENT_DATE",
    "HP_INSTALL_DATE",
    "FIRST_HP_MENTION",
    "LODGEMENT_DATETIME",
]


scotland_only_features = [
    "OSG_REFERENCE_NUMBER",
    "TYPE_OF_ASSESSMENT",
    "3_YR_ENERGY_COST_CURRENT",
    "3_YR_ENERGY_SAVING_POTENTIAL",
    "CURRENT_ENVIRONMENTAL_RATING",
    "POTENTIAL_ENVIRONMENTAL_RATING",
    "IMPROVEMENTS",
    "ALTERNATIVE_IMPROVEMENTS",
    "AIR_TIGHTNESS_DESCRIPTION",
    "AIR_TIGHTNESS_ENERGY_EFF",
    "AIR_TIGHTNESS_ENV_EFF",
    "LZC_ENERGY_SOURCES",
    "IMPACT_LOFT_INSULATION",
    "IMPACT_CAVITY_WALL_INSULATION",
    "IMPACT_SOLID_WALL_INSULATION",
    "ADDENDUM_TEXT",
    "DATA_ZONE",
    "WALL_DESCRIPTION",
    "SPACE_HEATING_DEMAND",
    "WATER_HEATING_DEMAND",
    "HEAT_LOSS_CORRIDOOR",
    "MAIN_HEATING_CATEGORY",
]


england_wales_only_features = [
    "LMK_KEY",
    "ADDRESS3",
    "LOCAL_AUTHORITY",
    "COUNTY",
    "FLAT_STOREY_COUNT",
    "ADDRESS",
    "LODGEMENT_DATE_TIME",
    "UPRN_SOURCE",
    "LODGEMENT_DATETIME",
]

scotland_field_fix_dict = {
    "BUILDING_REFERENCE_NUMBER": "ï»¿Property_UPRN",
    "UPRN": "OSG_UPRN",
    "POSTCODE": "Postcode",
    "INSPECTION_DATE": "Date of Assessment",
    "LODGEMENT_DATE": "Date of Certificate",
    "ENERGY_CONSUMPTION_CURRENT": "Primary Energy Indicator (kWh/mÂ²/year)",
    "TOTAL_FLOOR_AREA": "Total floor area (mÂ²)",
    "CO2_EMISS_CURR_PER_FLOOR_AREA": "CO2 Emissions Current Per Floor Area (kg.CO2/mÂ²/yr)",
    "CURRENT_ENERGY_EFFICIENCY": "Current energy efficiency rating",
    "CURRENT_ENERGY_RATING": "Current energy efficiency rating band",
    "POTENTIAL_ENERGY_EFFICIENCY": "Potential Energy Efficiency Rating",
    "POTENTIAL_ENERGY_RATING": "Potential energy efficiency rating band",
    "ENVIRONMENT_IMPACT_CURRENT": "Current Environmental Impact Rating",
    "ENVIRONMENT_IMPACT_POTENTIAL": "Potential Environmental Impact Rating",
    "CO2_EMISSIONS_CURRENT": "Current Emissions (T.CO2/yr)",
    "CO2_EMISSIONS_POTENTIAL": "Potential Reduction in Emissions (T.CO2/yr)",
    "CONSTRUCTION_AGE_BAND": "Part 1 Construction Age Band",
    "FLOOR_HEIGHT": "Part 1 Floor 0 Room Height",
    "ENERGY_CONSUMPTION_POTENTIAL": "Energy Consumption Potential",
    "EXTENSION_COUNT": "Extensions Count",
    "FIXED_LIGHTING_OUTLETS_COUNT": "Fixed Lighting Outlets Count",
    "LOW_ENERGY_FIXED_LIGHT_COUNT": "Low Energy Lighting Outlets Count",
    "LOW_ENERGY_LIGHTING": "Low Energy Lighting %",
    "FLOOR_LEVEL": "Flat Level",
    "FLAT_STOREY_COUNT": "Flat Location",
    "GLAZED_AREA": "Glazed Area",
    "NUMBER_HABITABLE_ROOMS": "Habitable Room Count",
    "HEAT_LOSS_CORRIDOR": "Heat Loss Corridor",
    "NUMBER_HEATED_ROOMS": "Heated Room Count",
    "LOCAL_AUTHORITY_LABEL": "Local Authority",
    "MAINS_GAS_FLAG": "Main Gas",
    "MAIN_FUEL": "Main Heating 1 Fuel Type",
    "MAIN_HEATING_CONTROLS": "Main Heating 1 Control",
    "MECHANICAL_VENTILATION": "Mechanical Ventilation",
    "ENERGY_TARIFF": "Meter Type",
    "MULTI_GLAZE_PROPORTION": "Multiple Glazed Proportion",
    "GLAZED_TYPE": "Multiple Glazing Type",
    "NUMBER_OPEN_FIREPLACES": "Open Fireplaces Count",
    "PHOTO_SUPPLY": "Photovoltaic Supply",
    "SOLAR_WATER_HEATING_FLAG": "Solar Water Heating",
    "TENURE": "Tenure",
    "TRANSACTION_TYPE": "Transaction Type",
    "UNHEATED_CORRIDOR_LENGTH": "Unheated Corridor Length",
    "CONSTITUENCY": "Ward Code",
    "CONSTITUENCY_LABEL": "Ward Name",
    "WIND_TURBINE_COUNT": "Wind Turbines Count",
    "BUILT_FORM": "Built Form",
    "PROPERTY_TYPE": "Property Type",
    "WALLS_DESCRIPTION": "WALL_DESCRIPTION",
    "WALLS_ENERGY_EFF": "WALL_ENERGY_EFF",
    "WALLS_ENV_EFF": "WALL_ENV_EFF",
    "POST_TOWN": "POSTTOWN",
    "POSTCODE": "Postcode",
}

rev_scotland_field_fix_dict = {v: k for k, v in scotland_field_fix_dict.items()}

# MCS settings

MCS_HP_PATH = "/inputs/MCS_data/mcs_heat_pumps.xlsx"
MCS_DOMESTIC_HP_PATH = "/outputs/mcs_domestic_hps.csv"
MCS_PROCESSED_PATH = "/outputs/data/mcs_processed.csv"
MCS_EPC_MERGED_PATH = "/outputs/data/mcs_epc.csv"
MCS_EPC_MATCHING_PARAMETER = 0.7
MCS_EPC_MAX_TOKEN_LENGTH = 8

MCS_MAX_COST = 200000
MCS_MAX_CAPACITY = 45
MCS_CLUSTER_TIME_INTERVAL = 31
MCS_EPC_ADDRESS_FIELDS = ["ADDRESS1", "ADDRESS2", "POSTCODE"]
MCS_EPC_CHARACTERISTIC_FIELDS = [
    "LMK_KEY",
    "ADDRESS1",
    "ADDRESS2",
    "POSTCODE",
    "INSPECTION_DATE",
    "TRANSACTION_TYPE",
    "TENURE",
    "CURRENT_ENERGY_RATING",
    "POTENTIAL_ENERGY_RATING",
    "PROPERTY_TYPE",
    "BUILT_FORM",
    "NUMBER_HABITABLE_ROOMS",
    "NUMBER_HEATED_ROOMS",
    "CONSTRUCTION_AGE_BAND",
    "TOTAL_FLOOR_AREA",
    "LIGHTING_ENERGY_EFF",
    "FLOOR_ENERGY_EFF",
    "WINDOWS_ENERGY_EFF",
    "WALLS_ENERGY_EFF",
    "ROOF_ENERGY_EFF",
    "MAINHEAT_DESCRIPTION",
]

MCS_INSTALLATIONS_PATH = "/outputs/MCS/mcs_installations_{}.csv"
MCS_INSTALLATIONS_EPC_FULL_PATH = "/outputs/MCS/mcs_installations_epc_full_{}.csv"
MCS_INSTALLATIONS_EPC_NEWEST_PATH = "/outputs/MCS/mcs_installations_epc_newest_{}.csv"
MCS_INSTALLATIONS_EPC_MOST_RELEVANT_PATH = (
    "/outputs/MCS/mcs_installations_epc_most_relevant_{}.csv"
)

MCS_RAW_INSTALLER_S3_PATH = "inputs/mcs/mcs_installer_information.xlsx"
MCS_RAW_INSTALLER_CONCAT_S3_PATH = "inputs/MCS/mcs_installers.csv"
PREPROC_GEO_MCS_INSTALLATIONS_PATH = (
    "/outputs/mcs/cleaned_geocoded_mcs_installations.csv"
)
PREPROC_MCS_INSTALLER_COMPANY_PATH = (
    "/outputs/mcs/cleaned_geocoded_mcs_installer_companies.csv"
)

LOCAL_NEW_MCS_DATA_DUMP_DIR = "/inputs/data/mcs/"
S3_NEW_MCS_DATA_DUMP_DIR = "inputs/MCS/latest_raw_data/"
INSTALLATIONS_RAW_S3_PATH = "inputs/MCS/mcs_installations.csv"
INSTALLATIONS_RAW_LOCAL_PATH = "/inputs/data/mcs/installations"
RAW_DATA_S3_FOLDER = "inputs/MCS/latest_raw_data"

EPC_MCS_MERGED_OUT_PATH = "/outputs/gold/merged_epc_mcs_installations_installers.csv"

MCS_INSTALLATIONS_FEAT_SELECTION = [
    "UPRN",
    "commission_date",
    "capacity",
    "estimated_annual_generation",
    "flow_temp",
    "tech_type",
    "scop",
    "design",
    "product_name",
    "manufacturer",
    "cost",
    "company_unique_id",
    "installer_name",
]

MCS_INSTALLER_FEAT_SELECTION = [
    "company_unique_id",
    "company_name",
    "mcs_certificate_number",
    "certification_body",
    "latitude",
    "longitude",
    "effective_from",
    "effective_to",
    "biomass_certified",
    "hydro_certified",
    "micro_chp_certified",
    "solar_pv_certified",
    "wind_turbine_certified",
    "solar_thermal_certified",
    "battery_storage_certified",
    "air_source_hp_certified",
    "ground_water_source_hp_certified",
    "hot_water_hp_certified",
    "exhaust_air_hp_certified",
    "gas_absorbtion_hp_certified",
    "solar_assisted_hp_certified",
]

# HISTORICAL INSTALLATIONS
historical_installations_rename_cols_dict = {
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
    "Installation Company MCS Number": "installation_company_mcs_number",
}


# HISTORICAL INSTALLERS
MCS_HISTORICAL_DATA_INPUTS_PATH = "inputs/MCS/latest_raw_data/historical/"

raw_historical_installers_dtypes = {
    "Company Name": str,
    "MCS certificate number": int,
    "Adddress 1": str,
    "Adddress 2": str,
    "Town": str,
    "County": str,
    "Postcode": str,
    "Effective From": datetime,
    "Effective To": datetime,
    "Air Source Heat Pump Installation Start Date": datetime,
    "Air Source Heat Pump Installation End Date": datetime,
    "Air Source Heat Pump Design Start Date": datetime,
    "Air Source Heat Pump Design End Date": datetime,
    "Biomass Start Date": datetime,
    "Biomass End Date": datetime,
    "Ground/Water Source Heat Pump Installation Start Date": datetime,
    "Ground/Water Source Heat Pump Installation End Date": datetime,
    "Ground/Water Source Heat Pump Design Start Date": datetime,
    "Ground/Water Source Heat Pump Design End Date": datetime,
    "Hot Water Heat Pump Installation Start Date": datetime,
    "Hot Water Heat Pump Installation End Date": datetime,
    "Hot Water Heat Pump Design Start Date": datetime,
    "Hot Water Heat Pump Design End Date": datetime,
    "Hydro Start Date": datetime,
    "Hydro End Date": datetime,
    "Micro CHP Start Date": datetime,
    "Micro CHP End Date": datetime,
    "Solar PV Start Date": datetime,
    "Solar PV End Date": datetime,
    "Wind Turbine Start Date": datetime,
    "Wind Turbine End Date": datetime,
    "Exhaust Air Heat Pump Installation Start Date": datetime,
    "Exhaust Air Heat Pump Installation End Date": datetime,
    "Exhaust Air Heat Pump Design Start Date": datetime,
    "Exhaust Air Heat Pump Design End Date": datetime,
    "Gas Absorbtion Heat Pump Installation Start Date": datetime,
    "Gas Absorbtion Heat Pump Installation End Date": datetime,
    "Gas Absorbtion Heat Pump Design Start Date": datetime,
    "Gas Absorbtion Heat Pump Design End Date": datetime,
    "Solar Assisted Heat Pump Installation Start Date": datetime,
    "Solar Assisted Heat Pump Installation End Date": datetime,
    "Solar Assisted Heat Pump Design Start Date": datetime,
    "Solar Assisted Heat Pump Design End Date": datetime,
    "Solar Thermal Start Date": datetime,
    "Solar Thermal End Date": datetime,
    "Battery Storage Start Date": datetime,
    "Battery Storage End Date": datetime,
}

raw_historical_installations_dtypes = {
    "Version Number": int,
    "Commissioning Date": datetime,
    "Address Line 1": str,
    "Address Line 2": str,
    "Address Line 3": str,
    "County": str,
    "Postcode": str,
    "Local Authority": str,
    "Total Installed Capacity": float,
    "Estimated Annual Generation": float,
    "Green Deal Installation?": str,
    "Installation Company Name": str,
    "Installation Company MCS Number": str,
    "Products": str,
    "Technology Type": str,
    "Installation New at Commissioning Date?": datetime,
    "Renewable System Design": str,
    "Annual Space Heating Demand": float,
    "Annual Water Heating Demand": float,
    "Annual Space Heating Supplied": float,
    "Annual Water Heating Supplied": float,
    "Number of MCS Certificates": float,
    "Heating System Type": str,
    "Fuel Type": str,
    "Overall Cost": float,
    "Installation Type": str,
}

MCS_HISTORICAL_DATA_OUTPUTS_PATH = "outputs/MCS/installers/"
PREPROCESSED_MCS_HISTORICAL_INSTALLERS_FILE_PATH = (
    "/outputs/MCS/installers/mcs_historical_installers_{}.csv"
)

processed_historical_installers_columns_order = [
    "company_unique_id",
    "company_name",
    "mcs_certificate_number",
    "certification_body",
    "address_1",
    "address_2",
    "town",
    "county",
    "postcode",
    "latitude",
    "longitude",
    "full_address",
    "original_record",
    "effective_from",
    "effective_to",
    "biomass_start_date",
    "biomass_end_date",
    "hydro_start_date",
    "hydro_end_date",
    "micro_chp_start_date",
    "micro_chp_end_date",
    "solar_pv_start_date",
    "solar_pv_end_date",
    "wind_turbine_start_date",
    "wind_turbine_end_date",
    "solar_thermal_start_date",
    "solar_thermal_end_date",
    "battery_storage_start_date",
    "battery_storage_end_date",
    "air_source_hp_start_date",
    "air_source_hp_end_date",
    "ground_water_source_hp_start_date",
    "ground_water_source_hp_end_date",
    "hot_water_hp_start_date",
    "hot_water_hp_end_date",
    "exhaust_air_hp_start_date",
    "exhaust_air_hp_end_date",
    "gas_absorbtion_hp_start_date",
    "gas_absorbtion_hp_end_date",
    "solar_assisted_hp_start_date",
    "solar_assisted_hp_end_date",
    "biomass_certified",
    "hydro_certified",
    "micro_chp_certified",
    "solar_pv_certified",
    "wind_turbine_certified",
    "solar_thermal_certified",
    "battery_storage_certified",
    "air_source_hp_certified",
    "ground_water_source_hp_certified",
    "hot_water_hp_certified",
    "exhaust_air_hp_certified",
    "gas_absorbtion_hp_certified",
    "solar_assisted_hp_certified",
]

preprocessed_historical_installers_dtypes = {
    "company_unique_id": str,
    "company_name": str,
    "mcs_certificate_number": int,
    "certification_body": str,
    "address_1": str,
    "address_2": str,
    "town": str,
    "county": str,
    "postcode": str,
    "latitude": float,
    "longitude": float,
    "full_address": str,
    "original_record": bool,
    "biomass_certified": bool,
    "hydro_certified": bool,
    "micro_chp_certified": bool,
    "solar_pv_certified": bool,
    "wind_turbine_certified": bool,
    "solar_thermal_certified": bool,
    "battery_storage_certified": bool,
    "air_source_hp_certified": bool,
    "ground_water_source_hp_certified": bool,
    "hot_water_hp_certified": bool,
    "exhaust_air_hp_certified": bool,
    "gas_absorbtion_hp_certified": bool,
    "solar_assisted_hp_certified": bool,
}

preprocessed_historical_installers_date_cols = [
    "effective_from",
    "effective_to",
    "biomass_start_date",
    "biomass_end_date",
    "hydro_start_date",
    "hydro_end_date",
    "micro_chp_start_date",
    "micro_chp_end_date",
    "solar_pv_start_date",
    "solar_pv_end_date",
    "wind_turbine_start_date",
    "wind_turbine_end_date",
    "solar_thermal_start_date",
    "solar_thermal_end_date",
    "battery_storage_start_date",
    "battery_storage_end_date",
    "air_source_hp_start_date",
    "air_source_hp_end_date",
    "ground_water_source_hp_start_date",
    "ground_water_source_hp_end_date",
    "hot_water_hp_start_date",
    "hot_water_hp_end_date",
    "exhaust_air_hp_start_date",
    "exhaust_air_hp_end_date",
    "gas_absorbtion_hp_start_date",
    "gas_absorbtion_hp_end_date",
    "solar_assisted_hp_start_date",
    "solar_assisted_hp_end_date",
]

MCS_PROCESSED_FILES_PATH = "/outputs/MCS/"
