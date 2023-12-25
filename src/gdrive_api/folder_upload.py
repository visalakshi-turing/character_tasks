import os
from typing import Optional

from googleapiclient.http import MediaFileUpload
from googleapiclient.discovery import Resource

from src.gdrive_api.utils import (
    get_nested_folder_id,
    get_file_id,
    create_folder_path,
    extract_folder_id,
)


class FolderNotFoundError(Exception):
    """Exception raised when the local source folder is not found."""
    pass


class UploadError(Exception):
    """Exception raised for errors that occur during file upload."""
    pass


def upload_file(
    service: Resource, file_path: str, parent_id: str, force_replace: bool = False
) -> Optional[str]:
    """Upload a file to Google Drive, optionally forcing replacement of existing files.

    Args:
        service: The Google Drive service resource.
        file_path: The path to the file to upload.
        parent_id: The ID of the parent folder in Google Drive.
        force_replace: If True, replace the file if it already exists.

    Returns:
        File url if the file was uploaded, None otherwise.
    """
    file_name = os.path.basename(file_path)
    file_metadata = {"name": file_name, "parents": [parent_id]}
    media = MediaFileUpload(file_path, resumable=True)
    file_id = get_file_id(service, file_name, parent_id)

    if file_id and not force_replace:
        print(f"File '{file_name}' already exists and won't be replaced.")
        return None

    file_url = None
    response = None

    if file_id and force_replace:
        print(f"Replacing existing file '{file_name}' with the new version.")
        response = service.files().update(fileId=file_id, media_body=media).execute()
        print(f"File '{file_name}' has been replaced.")
    else:
        print(f"Uploading new file '{file_name}'.")
        response = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )
        print(f"File '{file_name}' has been uploaded.")

    if response:
        file_url = f"https://drive.google.com/uc?id={response['id']}"
        print(f"Uploaded '{file_name}' to folder ID '{parent_id}'.")

    return file_url


def upload_folder(
    service: Resource,
    source_folder_path: str,
    destination_folder: str,
    force_replace: bool = False,
    is_url: bool = True,
) -> dict[str, str]:
    """Recursively upload a local folder to Google Drive.

    Args:
        service: The Google Drive service resource.
        source_folder_path: The path to the local folder to upload.
        destination_folder: The ID or URL of the destination folder in Google Drive.
        force_replace: If True, re-upload files even if they exist.
        is_url: A flag indicating whether the provided destination is a URL. Default is True.

    Raises:
        FolderNotFoundError: If the local folder does not exist.
        UploadError: If an error occurs during file upload.

    Returns:
        Dict of relative file path -> URL for the file after upload, URL is None if it was skipped due to force replace.
    """
    destination_folder_id = extract_folder_id(destination_folder, is_url)
    if not os.path.exists(source_folder_path):
        raise FolderNotFoundError(
            f"Local folder '{source_folder_path}' does not exist."
        )

    total_dirs = sum([len(dirs) for _, dirs, _ in os.walk(source_folder_path)])
    total_files = sum([len(files) for _, _, files in os.walk(source_folder_path)])
    dir_counter = 0
    file_counter = 0

    uploaded_files_count = 0
    skipped_files_count = 0
    uploaded_files = {}
    for root, dirs, files in os.walk(source_folder_path):
        dir_counter += 1
        relative_path = os.path.relpath(root, source_folder_path)
        current_folder_id = (
            destination_folder_id
            if relative_path == "."
            else get_nested_folder_id(service, relative_path, destination_folder_id)
        )

        if current_folder_id is None:
            current_folder_id = create_folder_path(
                service, relative_path, destination_folder_id
            )

        print("-" * 60)
        print(
            f"Processing directory {relative_path}: {dir_counter} of {total_dirs} in total."
        )
        for index, file_name in enumerate(files, start=1):
            file_counter += 1
            file_path = os.path.join(root, file_name)
            print(
                f"Uploading file {index} of {len(files)} in '{relative_path}', {file_counter} of {total_files} in total."
            )
            try:
                file_url = upload_file(
                    service, file_path, current_folder_id, force_replace
                )
                relative_file_path = os.path.relpath(file_path, source_folder_path)
                print(relative_file_path)
                print("=" * 90)
                if file_url is not None:
                    uploaded_files_count += 1
                    uploaded_files[relative_file_path] = file_url
                else:
                    skipped_files_count += 1
                    uploaded_files[relative_file_path] = None
            except Exception as e:
                raise UploadError(
                    f"An error occurred while uploading '{file_name}': {e}"
                )

    print("=" * 60)
    print(f"Successfully uploaded {uploaded_files_count} files out of {total_files}.")
    print(f"Skipped {skipped_files_count} files.")
    print(f"Successfully processed {total_dirs} directories.")
    print("=" * 60)
    return uploaded_files
