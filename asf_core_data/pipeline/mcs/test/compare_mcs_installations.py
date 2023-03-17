"""to run script,
can provide different old_df, new_df and comp_df s3 directory paths directly when running script.
defaults to paths in base_config
python compare_mcs_installations.py --new_installations_df NEW_INSTALLATIONS_PATH --old_installers_df OLD_INSTALLERS_PATH --new_installers_df NEW_INSTALLERS_PATH
"""

import datacompy
import pandera as pa
from datetime import datetime

from asf_core_data.getters.data_getters import s3, load_s3_data
import sys
import argparse

from asf_core_data.config import base_config

bucket_name = base_config.BUCKET_NAME


def compare_mcs_installers(old_installers_data, new_installers_data):

    compare = datacompy.Compare(
        old_installers_data,
        new_installers_data,
        join_columns=[
            "Company Name",
            "MCS certificate number",
            "Add 1",
            "PCode",
        ],
        df1_name="Old Installers Data",
        df2_name="New Installers Data",
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

    print(
        f"after dropping unspecified company name, we loose {len(mcs_installers) - len(mcs_installers_no_unspecified)} rows."
    )

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
            "Northern Ireland Region": pa.Column(
                checks=pa.Check.str_contains("YES|NO")
            ),
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

    try:
        mcs_installers_no_unspecified = schema_withchecks.validate(
            mcs_installers_no_unspecified, lazy=True
        )
    except pa.errors.SchemaErrors as err:
        err.failure_cases  # dataframe of schema errors
        err.data  # invalid dataframe


def within_mcs_installations_check(mcs_installations):

    schema_withchecks = pa.DataFrameSchema(
        columns={
            "Total Installed Capacity": pa.Column(float, nullable=True),
            "Estimated Annual Generation": pa.Column(float, nullable=True),
            "Annual Space Heating Demand": pa.Column(float, nullable=True),
            "Annual Water Heating Demand": pa.Column(float, nullable=True),
            "Annual Space Heating Supplied": pa.Column(float, nullable=True),
            "Annual Water Heating Supplied": pa.Column(float, nullable=True),
            "Overall Cost": pa.Column(float, nullable=True),
            "Certificate Creation Date": pa.Column(
                datetime,
                checks=[
                    pa.Check(lambda s: s.dt.year >= 2007),
                    pa.Check(lambda s: s.dt.year <= datetime.now().year),
                ],
            ),
            "Commissioning Date": pa.Column(
                datetime,
                checks=[
                    pa.Check(lambda s: s.dt.year >= 2007),
                    pa.Check(lambda s: s.dt.year <= datetime.now().year),
                ],
            ),
            "Version Number": pa.Column(int),
            "Postcode": pa.Column(checks=pa.Check.str_length(5, 8), nullable=True),
            "Technology Type": pa.Column(
                checks=pa.Check.isin(
                    [
                        "Air Source Heat Pump",
                        "Ground/Water Source Heat Pump",
                        "Exhaust Air Heat Pump",
                    ]
                ),
                nullable=True,
            ),
            "End User Installation Type": pa.Column(
                checks=pa.Check.isin(["Unspecified", "Domestic", "Non-Domestic"]),
                nullable=True,
            ),
            "Renewable System Design": pa.Column(
                checks=pa.Check.isin(
                    [
                        "Space heat and DHW",
                        "Unspecified",
                        "Space heat only",
                        "Space Heat, DHW and another purpose",
                        "DHW only",
                        "DHW and another purpose",
                        "Space Heat and another purpose",
                        "Another purpose only",
                    ]
                ),
                nullable=True,
            ),
            "Installation Requires Metering?": pa.Column(
                checks=pa.Check.isin(["Yes", "No"]), nullable=True
            ),
            "RHI Metering Status": pa.Column(
                checks=pa.Check.isin(
                    [
                        "Unspecified",
                        "Electricity meter(s) or on-board meter(s) installed for performance purposes",
                        "Electricity meter(s) or on-board meter(s) installed for performance purposes and Metering and Monitoring Service Package (MMSP) installed",
                        "No electricity or on-board meters installed",
                        "Electricity meter(s) already installed as part of a metering for payment purposes requirement",
                    ]
                ),
                nullable=True,
            ),
        },
        # unique=["Commissioning Date", "Address Line 1", "Address Line 2", "Address Line 3", "Postcode"],
    )

    try:
        schema_withchecks.validate(mcs_installations, lazy=True)
    except pa.errors.SchemaErrors as err:
        print(err.failure_cases)  # dataframe of schema errors
        # print(err.data)  # invalid dataframe


def test_installation_data(filename):

    raw_installation_data = load_s3_data(
        "asf-core-data", "inputs/MCS/latest_raw_data/" + filename
    )

    return within_mcs_installations_check(list(raw_installation_data.values())[0])


if __name__ == "__main__":
    test_output_txt = f'mcs_test_{datetime.now().strftime("%Y%m%d-%H%M%S")}.txt'

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-new_installations_df",
        "--new_installations_df",
        nargs="?",
        default=base_config.MCS_RAW_S3_PATH,
        help="directory of new_installations_df in s3",
    )
    parser.add_argument(
        "-old_installers_df",
        "--old_installers_df",
        nargs="?",
        default=base_config.MCS_RAW_INSTALLER_S3_PATH,
        help="directory of old_installers_df in s3",
    )

    parser.add_argument(
        "-new_installers_df",
        "--new_installers_df",
        nargs="?",
        help="directory of new_installers_df in s3",
    )

    args = parser.parse_args()
    new_installation_data_path = args.new_installations_df
    old_installers_path = args.old_installers_df
    new_installers_path = args.new_installers_df

    new_installation_data = load_s3_data(s3, bucket_name, new_installation_data_path)
    old_installers_data = load_s3_data(s3, bucket_name, old_installers_path)
    new_installers_data = load_s3_data(s3, bucket_name, new_installers_path)

    sys.stdout = open(test_output_txt, "w")

    print(f"---- within installer check of {new_installers_path}----")
    within_mcs_installers_check(new_installers_data)

    print(f"---- within installation check of {new_installation_data_path}----")
    within_mcs_installations_check(new_installation_data)

    print(
        f"---- between installations check of {old_installers_path} and {new_installers_path}----"
    )
    compare_mcs_installers(old_installers_data, new_installers_data)
