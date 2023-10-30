from __future__ import annotations

import dataclasses
import io
from abc import ABC, abstractmethod
from enum import Enum
from typing import Literal

import pandas as pd
import pendulum
from dagster import IOManager, OutputContext
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


# map a description to a google drive share id
class GoogleDriveDestination(Enum):
    NO_PHI_TEST = "0AIPsABP1BAIYUk9PVA"
    PHI_DATA_TEAM_REPORTING = "0ALbParwbfKqUUk9PVA"
    XDRIVE_CAHPS_WEEKLY = "1pXOy7yxDg7U9YIooK8WH0z0UkjajvgxG"
    XDRIVE_TMRP_MONTHLY = "1SR5bFWPp4KR8vSfBdPEV-zSB1IHmLn1F"
    XDRIVE_DMRP_WEEKLY = "1e0aT4595b0598wI4J1Utti5THAvyLqyJ"
    XDRIVE_DMRP_MONTHLY = "177_fnS-f0BWlLTO3kodgHbdOjuYFLPXY"
    XDRIVE_STATIN_OUTREACH_WEEKLY = "1L5vthcRGKeQ3nch9_U-kfJ07kQEZx9I_"


@dataclasses.dataclass
class GoogleDriveDirectoryInfo:
    id: str
    name: str


class FileNameConstructor:
    def __init__(self, pattern, custom_funcs=None):
        self.pattern = pattern
        self.custom_funcs = custom_funcs or {}

    def construct(self, **kwargs):
        file_name = self.pattern
        for key, value in kwargs.items():
            placeholder = "{" + key + "}"
            if key in self.custom_funcs:
                value = self.custom_funcs[key](value)
            file_name = file_name.replace(placeholder, str(value))

        return file_name


class GoogleDriveFile(ABC):
    def __init__(
        self,
        destination: GoogleDriveDestination,
        name: str,
        content: bytes,
        directory_path: str = "",
        append_date: bool = True,
    ):
        self.destination = destination

        if append_date:
            self.name = f"{name}_{pendulum.now().format('YYYYMMDD')}"
        else:
            self.name = name

        self.content = content
        # https://developers.google.com/drive/api/guides/ref-export-formats
        self.mime_type = self.get_mime_type()
        self.full_name = f"{self.name}.{self.get_extension()}"
        self.directory_path = directory_path

    @abstractmethod
    def get_extension(self) -> str:
        pass

    @abstractmethod
    def get_mime_type(self) -> str:
        pass

    def get_directory_path(self) -> str:
        return self.directory_path


class GoogleDriveTextFile(GoogleDriveFile):
    def __init__(self, destination: GoogleDriveDestination, name: str, content: bytes):
        super().__init__(destination, name, content)

    def get_extension(self) -> str:
        return "txt"

    def get_mime_type(self) -> str:
        return "text/plain"

    @classmethod
    def from_string(cls, destination: GoogleDriveDestination, name: str, content: str):
        return cls(destination, name, bytes(content, "utf-8"))


class GoogleDriveCsvFile(GoogleDriveFile):
    def __init__(
        self,
        destination: GoogleDriveDestination,
        name: str,
        content: bytes,
        file_extension: str = "csv",
        sep: str = ",",
        directory_path: str = "",
        append_date: bool = True,
    ):
        self.file_extension = file_extension
        self.sep = sep
        self.directory_path = directory_path
        super().__init__(destination, name, content, directory_path, append_date)

    def get_extension(self) -> str:
        return self.file_extension

    def get_mime_type(self) -> str:
        # Adjust the MIME type based on the extension
        if self.file_extension == "txt":
            return "text/plain"
        else:
            return "text/csv"

    @classmethod
    def from_dataframe(
        cls,
        destination: GoogleDriveDestination,
        name: str,
        data_frame: pd.DataFrame,
        file_extension: str = "csv",
        sep: str = ",",
        directory_path: str = "",
        append_date: bool = False,  # setting to false here so we can update other jobs in another PR
    ):
        return cls(
            destination,
            name,
            bytes(data_frame.to_csv(index=False, sep=sep), "utf-8"),
            file_extension,
            sep,
            directory_path,
            append_date,
        )

    @classmethod
    def from_dataframes(
        cls,
        destination: GoogleDriveDestination,
        data_frames: list[pd.DataFrame],
        columns_to_group: str | list[str],
        file_extension: str = "csv",
        directory_path: str = "",
        filename_constructor: FileNameConstructor | None = None,
        append_date: bool = True,
    ):
        google_drive_files = []

        # Ensure columns_to_group is a list
        if isinstance(columns_to_group, str):
            columns_to_group = [columns_to_group]

        for df in data_frames:
            grouped = df.groupby(columns_to_group)

            for names, group in grouped:
                # Ensure names is a tuple for consistency
                names = (*names,) if isinstance(names, str) else names

                # Check for length mismatch between columns_to_group and names
                if len(columns_to_group) != len(names):
                    raise ValueError("Mismatch between columns to group and names")

                # Use the FileNameConstructor if provided, otherwise default naming
                if filename_constructor:  # If a constructor is provided, use it
                    file_name = filename_constructor.construct(
                        **dict(zip(columns_to_group, names, strict=True))
                    )
                else:  # Fallback to default naming
                    file_name = "_".join(map(str, names))

                google_drive_file = cls.from_dataframe(
                    destination=destination,
                    name=file_name,
                    data_frame=group,
                    file_extension=file_extension,
                    directory_path=directory_path,
                    append_date=append_date,
                )
                google_drive_files.append(google_drive_file)

        return google_drive_files  # Returning list of GoogleDriveCSVFile instances


FindDirectoryCacheKey = tuple[str, str, str]


class GoogleDriveIOManager(IOManager):
    # !!! Do not sub-class ConfigurableIOManagerFactory !!!
    # Using ConfigurableIOManagerFactory will expose the auth_info in the dagster UI !!!
    def __init__(self, auth_info: dict[str, str]):
        self.auth_info = auth_info
        self.find_directory_cache: dict[FindDirectoryCacheKey, GoogleDriveDirectoryInfo | None] = {}

    def _get_drive_service(self):
        credentials = service_account.Credentials.from_service_account_info(self.auth_info)

        api_name = "drive"
        api_version = "v3"
        scope = "https://www.googleapis.com/auth/drive.file"
        scoped_credentials = credentials.with_scopes([scope])

        return build(api_name, api_version, credentials=scoped_credentials)

    def _find_directory(
        self, drive_id: str, name: str, parent_id: str = ""
    ) -> GoogleDriveDirectoryInfo | None:
        cache_key = (drive_id, name, parent_id)

        if cache_key in self.find_directory_cache:
            return self.find_directory_cache[cache_key]

        dir_info = self._find_directory_impl(drive_id, name, parent_id)
        self.find_directory_cache[cache_key] = dir_info
        return dir_info

    def _find_directory_impl(
        self, drive_id: str, name: str, parent_id: str = ""
    ) -> GoogleDriveDirectoryInfo | None:
        query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_id:
            query += f" and '{parent_id}' in parents"

        results = (
            self._get_drive_service()
            .files()
            .list(
                q=query,
                corpora="drive",
                driveId=drive_id,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            )
            .execute()
        )
        directories = results.get("files", [])

        # return None if no directories found
        if not directories:
            return None

        directory = directories[0]  # assuming the first directory is the one we want
        return GoogleDriveDirectoryInfo(id=directory["id"], name=directory["name"])

    def _create_directory_tree(self, destination: GoogleDriveDestination, path: str) -> str:
        # Set root directory node as the first parent
        return self._create_directory_tree_impl([{"name": "", "id": destination.value}], path.split("/"))

    def _create_directory_tree_impl(
        self, parents: list[dict[Literal["name", "id"], str]], directory_tree: list[str]
    ) -> str:
        # No more directory nodes to create in the directory tree, return the id of the leaf node
        if not directory_tree:
            return parents[-1]["id"]

        # Create next directory node in directory tree
        dir_info = self._create_directory(directory_tree[0], parents[-1]["id"], parents[0]["id"])

        # Mark the newly created directory node as the next parent, then recurse
        return self._create_directory_tree_impl(
            [*parents, {"name": dir_info.name, "id": dir_info.id}], directory_tree[1:]
        )

    def _create_directory(
        self, directory_name: str, parent_id: str, drive_id: str, error_on_existing_directory: bool = False
    ) -> GoogleDriveDirectoryInfo:
        # If the requested directory already exists, return the directory info
        existing_directory = self._find_directory(drive_id, directory_name, parent_id)
        if existing_directory is not None:
            if error_on_existing_directory:
                raise Exception(f"Directory {directory_name} with parent id {parent_id} already exists")

            return existing_directory

        # Otherwise, create a new directory
        new_directory = (
            self._get_drive_service()
            .files()
            .create(
                body={
                    "driveId": parent_id,
                    "parents": [parent_id],
                    "name": directory_name,
                    "mimeType": "application/vnd.google-apps.folder",
                },
                fields="id, name",
                supportsAllDrives=True,
            )
            .execute()
        )

        return GoogleDriveDirectoryInfo(id=new_directory.get("id"), name=new_directory.get("name"))

    def handle_output(self, context: OutputContext, files: GoogleDriveFile | list[GoogleDriveFile]):
        google_drive_file_ids = []
        google_drive_locations = []

        match files:
            case GoogleDriveFile():
                iterable_files = [files]
            case list():
                iterable_files = files

        for file in iterable_files:
            # The parent directory node is either the leaf subdirectory node, or the root directory node
            parent_id = (
                self._create_directory_tree(file.destination, file.directory_path)
                if file.directory_path
                else file.destination.value
            )

            file_metadata = {
                "driveId": parent_id,
                "parents": [parent_id],
                "name": file.full_name,
                "mimeType": file.mime_type,
            }

            media = MediaIoBaseUpload(
                io.BytesIO(file.content),
                mimetype=file.mime_type,
                resumable=True,
            )

            file_info = (
                self._get_drive_service()
                .files()
                .create(body=file_metadata, media_body=media, fields="id, name", supportsAllDrives=True)
                .execute()
            )

            google_drive_file_ids.append(file_info.get("id"))
            google_drive_locations.append(f"https://drive.google.com/drive/u/0/folders/{parent_id}")

        context.add_output_metadata({"google_drive_file_ids": google_drive_file_ids})
        context.add_output_metadata({"google_drive_locations": google_drive_locations})

    def load_input(self, context):
        pass
