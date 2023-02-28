"""Helper functions to run gdrive_to_s3 script."""

#############################################

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

#############################################
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

keywords = ["Reducing household", "Data", "MCS", "Data Dumps"]

all_folders = drive.ListFile(
    {
        "q": "'0AJY0sGMeo6M5Uk9PVA' in parents and trashed=false",
        "corpora": "teamDrive",
        "teamDriveId": "0AJY0sGMeo6M5Uk9PVA",
        "includeTeamDriveItems": True,
        "supportsTeamDrives": True,
    }
).GetList()


def get_folder_id(keyword: str, folder: list) -> list:
    """Helper function to get folder id based on keyword matching in folder title."""
    return [fold["id"] for fold in folder if keyword in fold["title"]]


def get_folder(folder_id: list):
    """Helper function to get folder based on folder id."""
    return drive.ListFile(
        {"q": "'{}' in parents and trashed=false".format(folder_id[0])}
    ).GetList()


def get_mcs_data_dumps_folder(keywords: list, all_folders: list) -> list:
    # writing this recursively results in a throttled API.
    """Function to get contents from Team's MCS Data Dump folder.
    Args:
        keywords (List): List of keywords to search folder title for.
        all_folers (List): Initial drive list of folders.
    Returns:
        mcs_data_dumps (List): drive folder with MCS data dumps.
    """
    reduce_households_folder = get_folder(get_folder_id(keywords[0], all_folders))
    data_folder = get_folder(get_folder_id(keywords[1], reduce_households_folder))
    mcs_folder = get_folder(get_folder_id(keywords[2], data_folder))
    mcs_data_dumps = get_folder(get_folder_id(keywords[3], mcs_folder))

    return mcs_data_dumps
