from asf_core_data import Path

MCS_RAW_S3_PATH = "inputs/MCS/mcs_heat_pumps.xlsx"
MCS_RAW_LOCAL_PATH = "/inputs/MCS/mcs_heat_pumps.xlsx"

CURRENT_YEAR = 2021
LATEST_BATCH = "Q4_2021_0721"

LATEST_V = "most_recent"
OTHER_V = "older_versions"

ROOT_DATA_PATH = "."

POSTCODE_TO_COORD_PATH = Path(
    "inputs/supplementary_data/geospatial/ukpostcodes_to_coordindates.csv"
)

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

KEPLER_OUTPUT = Path("outputs/figures/kepler/")

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
    "PROPERTY_TYPE": str,
    "INSPECTION_DATE": "datetime64[ns]",
    "LOCAL_AUTHORITY": str,
    "CONSTITUENCY": str,
    "COUNTY": str,
    "LODGEMENT_DATE": "datetime64[ns]",
    "TRANSACTION_TYPE": str,
    "ENVIRONMENT_IMPACT_CURRENT": int,
    "ENVIRONMENT_IMPACT_POTENTIAL": int,
    "ENERGY_CONSUMPTION_CURRENT": int,
    "ENERGY_CONSUMPTION_POTENTIAL": int,
    "CO2_EMISSIONS_CURRENT": float,
    "CO2_EMISSIONS_POTENTIAL": float,
    "CO2_EMISS_CURR_PER_FLOOR_AREA": float,
    "LIGHTING_COST_CURRENT": int,
    "LIGHTING_COST_POTENTIAL": int,
    "HEATING_COST_CURRENT": int,
    "HEATING_COST_POTENTIAL": int,
    "HOT_WATER_COST_CURRENT": int,
    "HOT_WATER_COST_POTENTIAL": int,
    "TOTAL_FLOOR_AREA": float,
    "ENERGY_TARIFF": str,
    "MAINS_GAS_FLAG": str,
    "FLOOR_LEVEL": str,
    "FLAT_TOP_STOREY": str,
    "FLAT_STOREY_COUNT": int,
    "MAIN_HEATING_CONTROLS": str,
    "MULTI_GLAZE_PROPORTION": int,
    "GLAZED_TYPE": str,
    "GLAZED_AREA": str,
    "EXTENSION_COUNT": int,
    "MECHANICAL_VENTILATION": str,
    "NUMBER_HABITABLE_ROOMS": int,
    "NUMBER_HEATED_ROOMS": int,
    "LOW_ENERGY_LIGHTING": int,
    "NUMBER_OPEN_FIREPLACES": int,
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
    "WIND_TURBINE_COUNT": int,
    "HEAT_LOSS_CORRIDOR": str,
    "UNHEATED_CORRIDOR_LENGTH": float,
    "FLOOR_HEIGHT": float,
    "PHOTO_SUPPLY": int,
    "SOLAR_WATER_HEATING_FLAG": str,
    "MECHANICAL_VENTILATION": str,
    "ADDRESS": str,
    "LOCAL_AUTHORITY_LABEL": str,
    "CONSTITUENCY_LABEL": str,
    "POSTTOWN": str,
    "CONSTRUCTION_AGE_BAND": str,
    "LODGEMENT_DATE": "datetime64[ns]",
    "TENRUE": str,
    "FIXED_LIGHTING_OUTLETS_COUNT": int,
    "LOW_ENERGY_FIXED_LIGHT_COUNT": int,
    "UPRN": str,
    "UPRN_SOURCE": str,
    # new features
    "COUNTRY": str,
    "N_ENTRIES": int,
    "HEATING_SYSTEM": str,
    "HEATING_FUEL": str,
    "HP_INSTALLED": bool,
    "HP_TYPE": str,
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
