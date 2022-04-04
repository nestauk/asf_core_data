from datetime import date

from asf_core_data import PROJECT_DIR, get_yaml_config, Path
from asf_core_data.pipeline.mcs.process.process_mcs_installations import (
    get_processed_mcs_data,
)
from asf_core_data.pipeline.mcs.process.mcs_epc_joining import (
    join_mcs_epc_data,
    select_most_relevant_epc,
)
from asf_core_data.getters.data_getters import s3, load_s3_data, save_to_s3

config = get_yaml_config(Path(str(PROJECT_DIR) + "/asf_core_data/config/base.yaml"))

bucket_name = config["BUCKET_NAME"]
mcs_installations_path = config["MCS_INSTALLATIONS_PATH"]
mcs_installations_epc_full_path = config["MCS_INSTALLATIONS_EPC_FULL_PATH"]
mcs_installations_epc_newest_path = config["MCS_INSTALLATIONS_EPC_NEWEST_PATH"]
mcs_installations_epc_most_relevant_path = config[
    "MCS_INSTALLATIONS_EPC_MOST_RELEVANT_PATH"
]

keyword_to_path_dict = {
    "none": mcs_installations_path,
    "full": mcs_installations_epc_full_path,
    "newest": mcs_installations_epc_newest_path,
    "most_relevant": mcs_installations_epc_most_relevant_path,
}


def generate_processed_mcs_installations(epc_version="none"):

    if epc_version not in ["none", "full", "newest", "most_relevant"]:
        raise ValueError(
            "epc_version should be one of 'none', 'full', 'newest' or 'most_relevant'"
        )

    processed_mcs = get_processed_mcs_data(save=False)

    if epc_version == "none":
        return processed_mcs
    else:
        joined_mcs_epc = join_mcs_epc_data(hps=processed_mcs, all_records=True)
        if epc_version == "newest":
            processed_mcs = joined_mcs_epc.loc[
                joined_mcs_epc.groupby("index_x")["INSPECTION_DATE"].idxmax()
            ]
        elif epc_version == "most_relevant":
            processed_mcs = select_most_relevant_epc(joined_mcs_epc)
        else:
            processed_mcs = joined_mcs_epc

    return processed_mcs


def get_mcs_installations(epc_version="none", refresh=False):

    if not refresh:
        bucket = s3.Bucket(bucket_name)
        folder = "outputs/MCS/"
        file_list = [object.key for object in bucket.objects.filter(Prefix=folder)]
        file_prefix = keyword_to_path_dict[epc_version]
        matches = [
            filename for filename in file_list if filename.startswith(file_prefix)
        ]
        if len(matches) > 0:
            latest_version = max(matches) + ".csv"
            print("Loading", latest_version, "from S3")
            mcs_installations = load_s3_data(
                s3, bucket_name, keyword_to_path_dict[epc_version]
            )
            return mcs_installations
        else:
            print("File not found on S3.")

    print("Generating required data...")
    mcs_installations = generate_processed_mcs_installations(epc_version=epc_version)

    return mcs_installations


def generate_and_save_mcs():
    today = date.today().strftime("%y%m%d")

    # Get all four versions and save them in S3

    no_epc_path, full_epc_path, newest_epc_path, most_relevant_epc_path = [
        (path_stem + "_" + today + ".csv")
        for path_stem in [
            mcs_installations_path,
            mcs_installations_epc_full_path,
            mcs_installations_epc_newest_path,
            mcs_installations_epc_most_relevant_path,
        ]
    ]

    processed_mcs = get_processed_mcs_data(save=False)
    save_to_s3(s3, bucket_name, processed_mcs, no_epc_path)
    print("Saved in S3: " + no_epc_path)

    fully_joined_mcs_epc = join_mcs_epc_data(hps=processed_mcs, all_records=True)
    save_to_s3(s3, bucket_name, fully_joined_mcs_epc, full_epc_path)
    print("Saved in S3: " + full_epc_path)

    newest_mcs_epc = fully_joined_mcs_epc.loc[
        fully_joined_mcs_epc.groupby("index_x")["INSPECTION_DATE"].idxmax()
    ]
    save_to_s3(s3, bucket_name, newest_mcs_epc, newest_epc_path)
    print("Saved in S3: " + newest_epc_path)

    most_relevant_mcs_epc = select_most_relevant_epc(fully_joined_mcs_epc)
    save_to_s3(s3, bucket_name, most_relevant_mcs_epc, most_relevant_epc_path)
    print("Saved in S3: " + most_relevant_mcs_epc)


if __name__ == "__main__":
    generate_and_save_mcs()
