import argparse
import os
from time import sleep

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContainerClient


def upload_file(container_client: ContainerClient, source: str, dest: str) -> None:
    """
    Upload a single file to a path inside the container.
    """
    print(f"Uploading {source} to {dest}")
    with open(source, "rb") as data:
        try:
            container_client.upload_blob(name=dest, data=data)
        except ResourceExistsError:
            pass


def upload_dir(container_client: ContainerClient, source: str, dest: str) -> None:
    """
    Upload a directory to a path inside the container.
    """
    prefix = "" if dest == "" else dest + "/"
    prefix += os.path.basename(source) + "/"
    for root, dirs, files in os.walk(source):
        for name in files:
            dir_part = os.path.relpath(root, source)
            dir_part = "" if dir_part == "." else dir_part + "/"
            file_path = os.path.join(root, name)
            blob_path = prefix + dir_part + name
            upload_file(container_client, file_path, blob_path)

def init_containers(
    service_client: BlobServiceClient, containers_directory: str
) -> None:
    """
    Iterate on the containers directory and do the following:
    1- create the container.
    2- upload all folders and files to the container.
    """
    for container_name in os.listdir(containers_directory):
        container_path = os.path.join(containers_directory, container_name)
        if os.path.isdir(container_path):
            container_client = service_client.get_container_client(container_name)
            try:
                container_client.create_container()
            except ResourceExistsError:
                pass
            for blob in os.listdir(container_path):
                blob_path = os.path.join(container_path, blob)
                if os.path.isdir(blob_path):
                    upload_dir(container_client, blob_path, "")
                else:
                    upload_file(container_client, blob_path, blob)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Initialize azurite emulator containers."
    )
    parser.add_argument(
        "--directory",
        required=True,
        help="""
        Directory that contains subdirectories named after the 
        containers that we should create. Each subdirectory will contain the files
         and directories of its container.
        """
    )

    args = parser.parse_args()

    # Connect to the localhost emulator (after 5 secs to make sure it's up).
    sleep(5)
    blob_service_client = BlobServiceClient(
        account_url="http://localhost:10000/devstoreaccount1",
        credential={
            "account_name": "devstoreaccount1",
            "account_key": (
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq"
                "/K1SZFPTOtr/KBHBeksoGMGw=="
            )
        }
    )

    # Only initialize if not already initialized.
    if next(blob_service_client.list_containers(), None):
        print("Emulator already has containers, will skip initialization.")
    else:
        init_containers(blob_service_client, args.directory)
