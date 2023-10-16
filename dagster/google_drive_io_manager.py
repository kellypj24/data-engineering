from __future__ import annotations

import dataclasses
import io
from abc import ABC, abstractmethod
from enum import Enum

import pandas as pd
import pendulum
from dagster import IOManager, OutputContext
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


# map a description to a google drive share id
class GoogleDriveDestination(Enum):
    GOOGLE_DRIVE_ID = "{google drive hash}"



@dataclasses.dataclass
class GoogleDriveFolderInfo:
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
        folder_path: str | None = None,
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
        self.folder_path = folder_path

    @abstractmethod
    def get_extension(self) -> str:
        pass

    @abstractmethod
    def get_mime_type(self) -> str:
        pass

    def get_folder_path(self) -> str | None:
        return self.folder_path


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


class GoogleDriveCSVFile(GoogleDriveFile):
    def __init__(
        self,
        destination: GoogleDriveDestination,
        name: str,
        content: bytes,
        file_extension: str = "csv",
        sep: str = ",",
        folder_path: str | None = None,
        append_date: bool = True,
    ):
        self.file_extension = file_extension
        self.sep = sep
        self.folder_path = folder_path
        super().__init__(destination, name, content, folder_path, append_date)

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
        folder_path: str | None = None,
        append_date: bool = False,  # setting to false here so we can update other jobs in another PR
    ):
        return cls(
            destination,
            name,
            bytes(data_frame.to_csv(index=False, sep=sep), "utf-8"),
            file_extension,
            sep,
            folder_path,
            append_date,
        )

    @classmethod
    def from_dataframes(
        cls,
        destination: GoogleDriveDestination,
        data_frames: list[pd.DataFrame],
        columns_to_group: str | list[str],
        file_extension: str = "csv",
        folder_path: str | None = None,
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
                    folder_path=folder_path,
                    append_date=append_date,
                )
                google_drive_files.append(google_drive_file)

        return google_drive_files  # Returning list of GoogleDriveCSVFile instances


class GoogleDriveIOManager(IOManager):
    # !!! Do not sub-class ConfigurableIOManagerFactory !!!
    # Using ConfigurableIOManagerFactory will expose the auth_info in the dagster UI !!!
    def __init__(self, auth_info: dict[str, str]):
        self.auth_info = auth_info

    def _get_drive_service(self):
        credentials = service_account.Credentials.from_service_account_info(self.auth_info)
        api_name = "drive"
        api_version = "v3"
        scope = "https://www.googleapis.com/auth/drive.file"
        scoped_credentials = credentials.with_scopes([scope])
        return build(api_name, api_version, credentials=scoped_credentials)

    def _find_folder(
        self, drive_id: str, name: str, parent_id: str | None = None
    ) -> GoogleDriveFolderInfo | None:
        query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        # added trashed=false
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
        folders = results.get("files", [])
        if folders:
            folder = folders[0]  # assuming the first folder is the one we want
            return GoogleDriveFolderInfo(id=folder["id"], name=folder["name"])
        return None  # Explicitly return None when no folder is found

    def _create_directory(self, parent_id: str, name: str) -> GoogleDriveFolderInfo:
        file_metadata = {
            "driveId": parent_id,
            "parents": [parent_id],
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
        }
        folder = (
            self._get_drive_service()
            .files()
            .create(body=file_metadata, fields="id, name", supportsAllDrives=True)
            .execute()
        )
        return GoogleDriveFolderInfo(id=folder.get("id"), name=folder.get("name"))

    def handle_output(self, context: OutputContext, files: GoogleDriveFile | list[GoogleDriveFile]):
        folder_cache: dict[str, str] = {}
        google_drive_file_ids = []
        google_drive_locations = []

        match files:
            case GoogleDriveFile():
                iterable_files = [files]
            case list():
                iterable_files = files

        for file in iterable_files:
            parent_id = file.destination.value
            if file.folder_path:
                if file.folder_path in folder_cache:
                    folder_id = folder_cache[file.folder_path]
                    parent_id = folder_id  # Update parent_id here as well
                else:
                    existing_folder = self._find_folder(
                        drive_id=file.destination.value, name=file.folder_path
                    )
                    if existing_folder:
                        context.log.info(
                            f"found existing folder {file.folder_path} / id: {existing_folder.id}"
                        )
                        folder_id = existing_folder.id
                        parent_id = folder_id  # Update parent_id here
                    else:
                        context.log.info(f"creating folder {file.folder_path}")
                        folder_info = self._create_directory(file.destination.value, file.folder_path)
                        folder_id = folder_info.id
                        parent_id = folder_id  # This line updates parent_id when a new folder is created
                    folder_cache[file.folder_path] = folder_id  # Update folder_cache

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
