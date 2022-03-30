import pandas as pd
import datacompy

from asf_core_data import PROJECT_DIR, get_yaml_config, Path
from asf_core_data.getters.data_getters import s3, load_s3_data

config = get_yaml_config(Path(str(PROJECT_DIR) + "/asf_core_data/config/base.yaml"))

bucket_name = config["BUCKET_NAME"]


def compare_mcs(old_data_path, new_data_path):

    old_data = load_s3_data(s3, bucket_name, old_data_path)
    new_data = load_s3_data(s3, bucket_name, new_data_path)

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


if __name__ == "__main__":
    compare_mcs(
        "inputs/MCS/mcs_heat_pumps_sept_21.xlsx", "inputs/MCS/mcs_heat_pumps.xlsx"
    )
