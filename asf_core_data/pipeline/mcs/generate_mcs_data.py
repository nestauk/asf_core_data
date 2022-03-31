from datetime import date

from asf_core_data import PROJECT_DIR, get_yaml_config, Path
from asf_core_data.pipeline.mcs.process.process_mcs_installations import (
    get_processed_mcs_data,
)
from asf_core_data.pipeline.mcs.process.mcs_epc_joining import (
    join_mcs_epc_data,
    select_most_relevant_epc,
)
from asf_core_data.getters.data_getters import s3, save_to_s3

config = get_yaml_config(Path(str(PROJECT_DIR) + "/asf_core_data/config/base.yaml"))

bucket_name = config["BUCKET_NAME"]
mcs_installations_path = config["MCS_INSTALLATIONS_PATH"]
mcs_installations_epc_full_path = config["MCS_INSTALLATIONS_EPC_FULL_PATH"]
mcs_installations_epc_newest_path = config["MCS_INSTALLATIONS_EPC_NEWEST_PATH"]
mcs_installations_epc_most_relevant_path = config[
    "MCS_INSTALLATIONS_EPC_MOST_RELEVANT_PATH"
]


def generate_processed_mcs(epc_version, geocode=True):

    if epc_version not in ["none", "full", "newest", "most_relevant"]:
        raise ValueError(
            "epc_version should be one of 'none', 'full', 'newest' or 'most_relevant'"
        )
    if epc_version == "none":
        processed_mcs = get_processed_mcs_data(save=False)
    else:
        joined_mcs_epc = join_mcs_epc_data(save=False, all_records=True)
        if epc_version == "newest":
            processed_mcs = joined_mcs_epc.loc[
                joined_mcs_epc.groupby("index_x")["INSPECTION_DATE"].idxmax()
            ]
        elif epc_version == "most_relevant":
            processed_mcs = select_most_relevant_epc(joined_mcs_epc)
        else:
            processed_mcs = joined_mcs_epc

    if geocode:
        print("geocoding not yet implemented")
        # add geocoding here

    return processed_mcs


if __name__ == "__main__":
    # Get today's date for filename suffix
    today = date.today().strftime("%y%m%d")
    # Get all four versions and save them in S3
    for parameter, path in [
        ("none", mcs_installations_path),
        ("full", mcs_installations_epc_full_path),
        ("newest", mcs_installations_epc_newest_path),
        ("most_relevant", mcs_installations_epc_most_relevant_path),
    ]:
        data = generate_processed_mcs(epc_version=parameter)
        full_path = path + "_" + today + ".csv"
        save_to_s3(s3, bucket_name, data, full_path)
        print("Saved in S3: " + full_path)
