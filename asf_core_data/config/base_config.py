from asf_core_data import Path

MCS_RAW_S3_PATH = "inputs/MCS/mcs_heat_pumps.xlsx"
MCS_RAW_LOCAL_PATH = "/inputs/MCS/mcs_heat_pumps.xlsx"

CURRENT_YEAR = 2021
LATEST_BATCH = "Q4_2021_0721"

LATEST_V = "most_recent"
OTHER_V = "older_versions"

ROOT_DATA_PATH = "."

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
PREPROC_EPC_DATA_PATH = Path("outputs/EPC/preprocessed_data/{}/EPC_GB_preprocessed.csv")

PROCESSED_EPC_DATA_PATH = Path("./outputs/EPC/preprocessed_data/{}/")

SNAPSHOT_RAW_EPC_DATA_PATH = Path("inputs/EPC/preprocessed_data/{}/EPC_GB_raw.csv")
SNAPSHOT_PREPROC_EPC_DATA_DEDUPL_PATH = Path(
    "inputs/EPC/preprocessed_data/{}/EPC_GB_preprocessed_and_deduplicated.csv"
)
SNAPSHOT_PREPROC_EPC_DATA_PATH = Path(
    "/EPC/preprocessed_data/{}/EPC_GB_preprocessed.csv"
)

RAW_ENG_WALES_DATA_ZIP = Path(
    "inputs/EPC/raw_data/{}/England_Wales/all-domestic-certificates.zip"
)
RAW_SCOTLAND_DATA_ZIP = Path("inputs/EPC/raw_data/{}/Scotland/D_EPC_data.zip")

RAW_DATA_PATH = Path("inputs/EPC/raw_data/{}/")

RAW_ENG_WALES_DATA_PATH = Path("inputs/EPC/raw_data/{}/England_Wales/")
RAW_SCOTLAND_DATA_PATH = Path("inputs/EPC/raw_data/{}/Scotland/")

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

EPC_FEAT_SELECTION = [
    "ADDRESS1",
    "POSTCODE",
    "POSTTOWN",
    "MAINS_GAS_FLAG",
    "NUMBER_HABITABLE_ROOMS",
    "LOCAL_AUTHORITY_LABEL",
    "TRANSACTION_TYPE",
    "CURRENT_ENERGY_RATING",
    "POTENTIAL_ENERGY_RATING",
    "CURRENT_ENERGY_EFFICIENCY",
    "ENERGY_CONSUMPTION_CURRENT",
    "TENURE",
    "MAINHEAT_ENERGY_EFF",
    "SHEATING_ENERGY_EFF",
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
    "LIGHTING_COST_CURRENT",
    "LIGHTING_COST_POTENTIAL",
    "HEATING_COST_CURRENT",
    "HEATING_COST_POTENTIAL",
    "HOT_WATER_COST_CURRENT",
    "HOT_WATER_COST_POTENTIAL",
    "BUILDING_REFERENCE_NUMBER",
    "LODGEMENT_DATE",
    "INSPECTION_DATE",
    "BUILT_FORM",
    "PROPERTY_TYPE",
    "CONSTRUCTION_AGE_BAND",
    "TRANSACTION_TYPE",
    "MAIN_FUEL",
    "TOTAL_FLOOR_AREA",
    "ENERGY_TARIFF",
    "UPRN",
    "SECONDHEAT_DESCRIPTION",
]

EPC_PREPROC_FEAT_SELECTION = [
    "BUILDING_REFERENCE_NUMBER",
    "ADDRESS1",
    "ADDRESS2",
    "POSTTOWN",
    "POSTCODE",
    "INSPECTION_DATE",
    "LODGEMENT_DATE",
    "ENERGY_CONSUMPTION_CURRENT",
    "TOTAL_FLOOR_AREA",
    "CURRENT_ENERGY_EFFICIENCY",
    "CURRENT_ENERGY_RATING",
    "POTENTIAL_ENERGY_RATING",
    "CO2_EMISS_CURR_PER_FLOOR_AREA",
    "WALLS_ENERGY_EFF",
    "ROOF_ENERGY_EFF",
    "FLOOR_ENERGY_EFF",
    "WINDOWS_ENERGY_EFF",
    "MAINHEAT_DESCRIPTION",
    "MAINHEAT_ENERGY_EFF",
    "MAINHEATC_ENERGY_EFF",
    "SHEATING_ENERGY_EFF",
    "HOT_WATER_ENERGY_EFF",
    "LIGHTING_ENERGY_EFF",
    "CO2_EMISSIONS_CURRENT",
    "HEATING_COST_CURRENT",
    "HEATING_COST_POTENTIAL",
    "HOT_WATER_COST_CURRENT",
    "HOT_WATER_COST_POTENTIAL",
    "LIGHTING_COST_CURRENT",
    "LIGHTING_COST_POTENTIAL",
    "CONSTRUCTION_AGE_BAND",
    "CONSTRUCTION_AGE_BAND_ORIGINAL",
    "NUMBER_HABITABLE_ROOMS",
    "LOCAL_AUTHORITY_LABEL",
    "MAINS_GAS_FLAG",
    "MAIN_FUEL",
    "ENERGY_TARIFF",
    "TENURE",
    "TRANSACTION_TYPE",
    "BUILT_FORM",
    "PROPERTY_TYPE",
    "COUNTRY",
    "INSPECTION_DATE",
    "UNIQUE_ADDRESS",
    "BUILDING_ID",
    "N_ENTRIES",
    "N_ENTRIES_BUILD_ID",
    "HEATING_SYSTEM",
    "HEATING_FUEL",
    "HP_INSTALLED",
    "HP_TYPE",
    "CURR_ENERGY_RATING_NUM",
    "ENERGY_RATING_CAT",
    "DIFF_POT_ENERGY_RATING",
    "UPRN",
    "SECONDHEAT_DESCRIPTION",
]

dtypes = {
    "BUILDING_REFERENCE_NUMBER": int,
    "ADDRESS1": str,
    "ADDRESS2": str,
    "POSTTOWN": str,
    "POSTCODE": str,
    "INSPECTION_DATE": "datetime64[ns]",
    "LODGEMENT_DATE": "datetime64[ns]",
    "ENERGY_CONSUMPTION_CURRENT": float,
    "TOTAL_FLOOR_AREA": float,
    "CURRENT_ENERGY_EFFICIENCY": int,
    "CURRENT_ENERGY_RATING": str,
    "POTENTIAL_ENERGY_RATING": str,
    "CO2_EMISS_CURR_PER_FLOOR_AREA": float,
    "WALLS_ENERGY_EFF": str,
    "ROOF_ENERGY_EFF": str,
    "FLOOR_ENERGY_EFF": str,
    "WINDOWS_ENERGY_EFF": str,
    "MAINHEAT_DESCRIPTION": str,
    "MAINHEAT_ENERGY_EFF": str,
    "MAINHEATC_ENERGY_EFF": str,
    "SHEATING_ENERGY_EFF": str,
    "HOT_WATER_ENERGY_EFF": str,
    "LIGHTING_ENERGY_EFF": str,
    "CO2_EMISSIONS_CURRENT": float,
    "HEATING_COST_CURRENT": float,
    "HEATING_COST_POTENTIAL": float,
    "HOT_WATER_COST_CURRENT": float,
    "HOT_WATER_COST_POTENTIAL": float,
    "LIGHTING_COST_CURRENT": float,
    "LIGHTING_COST_POTENTIAL": float,
    "CONSTRUCTION_AGE_BAND": str,
    "FLOOR_HEIGHT": float,
    "EXTENSION_COUNT": float,
    "FLOOR_LEVEL": float,
    "GLAZED_AREA": float,
    "NUMBER_HABITABLE_ROOMS": str,
    "NUMBER_HEATED_ROOMS": str,
    "LOCAL_AUTHORITY_LABEL": str,
    "MAINS_GAS_FLAG": str,
    "MAIN_FUEL": str,
    "MAIN_HEATING_CONTROLS": float,
    "MECHANICAL_VENTILATION": str,
    "ENERGY_TARIFF": str,
    "MULTI_GLAZE_PROPORTION": float,
    "GLAZED_TYPE": str,
    "PHOTO_SUPPLY": float,
    "SOLAR_WATER_HEATING_FLAG": str,
    "TENURE": str,
    "TRANSACTION_TYPE": str,
    "WIND_TURBINE_COUNT": float,
    "BUILT_FORM": str,
    "PROPERTY_TYPE": str,
    "COUNTRY": str,
    "CONSTRUCTION_AGE_BAND_ORIGINAL": str,
    "ENTRY_YEAR": str,
    "ENTRY_YEAR_INT": float,
    "UNIQUE_ADDRESS": str,
    "BUILDING_ID": str,
    "N_ENTRIES": str,
    "N_ENTRIES_BUILD_ID": str,
    "HEATING_SYSTEM": str,
    "HEATING_FUEL": str,
    "HP_INSTALLED": bool,
    "HP_TYPE": str,
    "CURR_ENERGY_RATING_NUM": float,
    "ENERGY_RATING_CAT": str,
    "DIFF_POT_ENERGY_RATING": float,
}

MCS_HP_PATH = Path("/inputs/MCS_data/mcs_heat_pumps.xlsx")
MCS_DOMESTIC_HP_PATH = Path("/outputs/mcs_domestic_hps.csv")
INFLATION_PATH = Path("/inputs/MCS_data/inflation.csv")
MCS_EPC_MATCHING_PARAMETER = 0.7
MCS_EPC_MERGED_PATH = Path("/outputs/mcs_epc.csv")
MCS_EPC_MAX_TOKEN_LENGTH = 8

parse_dates = [
    "INSPECTION_DATE",
    "LODGEMENT_DATE",
    "HP_INSTALL_DATE",
    "FIRST_HP_MENTION",
]
