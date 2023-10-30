from datetime import datetime

import pandas as pd
from dagster import asset

from cureatr.io_managers.google_drive_io_manager import (
    FileNameConstructor,
    GoogleDriveCsvFile,
    GoogleDriveDestination,
    GoogleDriveTextFile,
)


@asset(group_name="test_files", io_manager_key="google_drive_io_manager")
def test_text_file():
    return GoogleDriveTextFile.from_string(
        destination=GoogleDriveDestination.NO_PHI_TEST,
        name="from_string",
        content="hello from test-file!",
    )


@asset(group_name="test_files", io_manager_key="google_drive_io_manager")
def test_csv_file():
    df = pd.DataFrame(
        {"name": ["Raphael", "Donatello"], "mask": ["red", "purple"], "weapon": ["sai", "bo staff"]}
    )
    return GoogleDriveCsvFile.from_dataframe(
        destination=GoogleDriveDestination.NO_PHI_TEST,
        name="from_data_frame",
        data_frame=df,
        file_extension="csv",
        directory_path="test_folder2",
    )


# In your test_files.py
@asset(group_name="test_files", io_manager_key="google_drive_io_manager")
def test_dialer_split():
    df = pd.DataFrame(
        {
            "name": ["Raphael", "Donatello", "Michaelangelo", "Raphael"],
            "mask": ["red", "purple", "orange", "blue"],
            "weapon": ["sai", "bo staff", "nunchucks", "katana"],
        }
    )
    # Define custom filename constructor
    constructor_pattern = "ninja_turtles_{name}"
    filename_constructor = FileNameConstructor(constructor_pattern)

    # Convert single DataFrame to list as input
    google_drive_files = GoogleDriveCsvFile.from_dataframes(
        destination=GoogleDriveDestination.NO_PHI_TEST,
        data_frames=[df],
        columns_to_group="name",
        directory_path="splinters_students",
        filename_constructor=filename_constructor,
        append_date=False,
    )

    return google_drive_files  # Returning list of GoogleDriveCSVFile instances directly


@asset(group_name="test_files", io_manager_key="google_drive_io_manager")
def test_create_directory_tree():
    dataframe = pd.DataFrame(
        {
            "column_A": ["foo", "bar"],
            "column_B": ["foo", "bar"],
            "column_C": ["foo", "bar"],
        }
    )

    return GoogleDriveCsvFile.from_dataframe(
        destination=GoogleDriveDestination.NO_PHI_TEST,
        data_frame=dataframe,
        name=f"{datetime.now().strftime('%Y%m%d%H%M%S')}_test_create_directory_tree",
        file_extension="csv",
        directory_path="tests/subdir_A/subdir_B",
    )
