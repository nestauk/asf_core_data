########################################
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

import gdown
import os
from datetime import datetime

from asf_core_data.getters.data_getters import s3, save_to_s3

from asf_core_data import config, bucket_name, PROJECT_DIR

########################################

local_data_dump_dir = config["lOCAL_NEW_MCS_DATA_DUMP_DIR"]
s3_data_dump_dir = config["S3_NEW_MCS_DATA_DUMP_DIR"]

gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)


def drive_to_s3(local_data_dump_dir, s3_data_dump_dir):
    # get all folder names
    all_folders_list = drive.ListFile(
        {"q": "'root' in parents and trashed=false"}
    ).GetList()
    # get id of MCS Data Dumps folder name
    mcs_folder_id = [
        folder["id"]
        for folder in all_folders_list
        if folder["title"] == "MCS Data Dumps"
    ]
    # get mcs files and parse created date
    mcs_files = drive.ListFile(
        {"q": "'{}' in parents and trashed=false".format("".join(mcs_folder_id))}
    ).GetList()
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

    if not os.path.exists(str(PROJECT_DIR) + local_data_dump_dir):
        os.mkdir(str(PROJECT_DIR) + local_data_dump_dir)

    for _ in (latest_installations, latest_installers):
        name, ext = _["title"].split(".")[0], _["title"].split(".")[1]
        timestamp = str(_["createdDate"]).split(" ")[0].replace("-", "_")
        latest_dump_name = name + "_" + timestamp + "." + ext
        output_path = str(PROJECT_DIR) + local_data_dump_dir + latest_dump_name
        s3_path = s3_data_dump_dir + latest_dump_name
        file_id = _["id"]
        gdown.download(id=file_id, output=output_path, quiet=False)
        s3.Bucket(bucket_name).upload_file(output_path, s3_path)


if __name__ == "__main__":
    drive_to_s3(local_data_dump_dir, s3_data_dump_dir)
