import datacompy
import pandas as pd
import pandera as pa
from datetime import datetime
import time

from asf_core_data import PROJECT_DIR, get_yaml_config, Path
from asf_core_data.getters.data_getters import s3, load_s3_data
import sys
import argparse

config = get_yaml_config(Path(str(PROJECT_DIR) + "/asf_core_data/config/base.yaml"))

bucket_name = config["BUCKET_NAME"]

old_data_path = "inputs/MCS/mcs_heat_pumps_sept_21.xlsx"
new_data_path = config["MCS_RAW_S3_PATH"]
installers_path = config["MCS_RAW_INSTALLER_S3_PATH"]

old_data = load_s3_data(s3, bucket_name, old_data_path)
new_data = load_s3_data(s3, bucket_name, new_data_path)
mcs_installers = load_s3_data(s3, bucket_name, installers_path)

def compare_mcs(old_data, new_data):

    compare = datacompy.Compare(
        old_data,
        new_data,
        join_columns=[
            "Version Number",
            "Commissioning Date",
            "Address Line 1",
            "Postcode",
        ],
        df1_name="Old Data",
        df2_name="New Data",
    )

    print(compare.report())

def within_mcs_installers_check(mcs_installers):

    assert len(list(set(mcs_installers["Company Name"]))) == len(
        mcs_installers
    ), "each row is not a MCS registered company"

    print(f"the NA summary is {mcs_installers.isna().sum()}.")

    # drop 'Unspecified' Company Name
    mcs_installers_no_unspecified = mcs_installers[
        mcs_installers["Company Name"] != "Unspecified"
    ]

    print(f"after dropping unspecified company name, we loose {len(mcs_installers) - len(mcs_installers_no_unspecified)} rows.")

    schema_withchecks = pa.DataFrameSchema(
    {
        "Company Name": pa.Column(str),
        "MCS certificate number": pa.Column(str, checks=pa.Check.str_contains("-")),
        "Add 1": pa.Column(str),
        "Add2": pa.Column(str, nullable=True),
        "Town": pa.Column(str, nullable=True),
        "County": pa.Column(str, nullable=True),
        "PCode": pa.Column(str, checks=pa.Check.str_length(6, 8)),
        "Solar Thermal": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "Wind Turbines": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "Air Source Heat Pumps": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "Exhaust Air Heat Pumps": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "Biomass": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "Solar Photovoltaics": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "Micro CHP": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "SolarAssistedHeatPump": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "GasAbsorptionHeatPump": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "Ground/Water Source Heat Pump": pa.Column(
            checks=pa.Check.str_contains("YES|NO")
        ),
        "Battery Storage": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "Eastern Region": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "East Midlands Region": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "London Region": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "North East Region": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "North West Region": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "South East Region": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "South West Region": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "West Midlands Region": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "Yorkshire Humberside Region": pa.Column(
            checks=pa.Check.str_contains("YES|NO")
        ),
        "Northern Ireland Region": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "Scotland Region": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "Wales Region": pa.Column(checks=pa.Check.str_contains("YES|NO")),
        "Effective From": pa.Column(
            checks=[
                pa.Check(lambda s: s.dt.year >= 1900),
                pa.Check(lambda s: s.dt.year <= datetime.now().year),
            ]
        ),
        "Consumer Code": pa.Column(str, checks=pa.Check.str_length(3, 4)),
        "Certification Body": pa.Column(str),
        }
    )

    mcs_installer_comp_data = schema_withchecks.validate(mcs_installers_no_unspecified, lazy=True)

if __name__ == "__main__":
    test_output_txt = str(PROJECT_DIR) + f'/asf_core_data/pipeline/mcs/test/mcs_test_{datetime.now().strftime("%Y%m%d-%H%M%S")}.txt'

    sys.stdout = open(test_output_txt, 'w')
    print(f'---- within intaller check of {installers_path}----')
    within_mcs_installers_check(mcs_installers)
    print(f'---- between installations check of {old_data_path} and {new_data_path}----')
    compare_mcs(old_data, new_data)
    