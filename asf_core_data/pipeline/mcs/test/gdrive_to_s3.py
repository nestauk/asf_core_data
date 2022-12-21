########################################
import os
from datetime import datetime

from asf_core_data.getters.data_getters import s3

from asf_core_data.config import base_config

from asf_core_data.pipeline.mcs.test.gdrive_to_s3_utils import (
    drive,
    keywords,
    all_folders,
    get_mcs_data_dumps_folder,
)

########################################

local_data_dump_dir = base_config.LOCAL_NEW_MCS_DATA_DUMP_DIR
s3_data_dump_dir = base_config.S3_NEW_MCS_DATA_DUMP_DIR
bucket_name = base_config.BUCKET_NAME


def drive_to_s3(local_data_dump_dir, s3_data_dump_dir):
    """Pulls MCS data dumps from Google Drive to a local dir and s3.
    Inputs:
        local_data_dump_dir (str): local path to store MCS data dumps.
        s3_data_dump_dir (str): path in s3 to store MCS data dumps.
    """
    mcs_files = get_mcs_data_dumps_folder(keywords, all_folders)

    installers = []
    installations = []
    for mcs_file in mcs_files:
        time = mcs_file["createdDate"].replace("T", " ").replace("Z", "")
        mcs_file["createdDate"] = datetime.strptime(time, "%Y-%m-%d %H:%M:%S.%f")
        if "installations" in mcs_file["title"]:
            installations.append(mcs_file)
        elif "installer" in mcs_file["title"]:
            installers.append(mcs_file)

    latest_installations, latest_installers = [
        max(_, key=lambda x: x["createdDate"]) for _ in (installations, installers)
    ]

    if not os.path.exists(base_config.ROOT_DATA_PATH + local_data_dump_dir):
        os.mkdir(base_config.ROOT_DATA_PATH + local_data_dump_dir)

    for _ in (latest_installations, latest_installers):
        name, ext = _["title"].split(".")[0], _["title"].split(".")[1]
        print(name, ext)
        timestamp = str(_["createdDate"]).split(" ")[0].replace("-", "_")
        latest_dump_name = name + "_" + timestamp + "." + ext
        output_path = (
            base_config.ROOT_DATA_PATH + local_data_dump_dir + latest_dump_name
        )
        s3_path = s3_data_dump_dir + latest_dump_name
        file_id = _["id"]
        file = drive.CreateFile({"id": file_id})
        print(f"downloaded file {file_id} locally...")
        print(f"pushing file {file_id} to s3...")
        file.GetContentFile(output_path)
        s3.Bucket(bucket_name).upload_file(output_path, s3_path)


if __name__ == "__main__":
    drive_to_s3(local_data_dump_dir, s3_data_dump_dir)
    print("google drive to s3 complete!")
