#!/bin/env python3

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ClientAuthenticationError, ServiceRequestError
import os
import glob
import datetime
import time
import argparse
import yaml


def create_example_config_file(file_path):
    if os.path.exists(file_path):
        print(
            f"Error: File '{file_path}' already exists. Please specify a different location."
        )
        return False

    example_config = {
        "storage_account_name": "your_storage_account_name",
        "storage_account_key": "your_storage_account_key",
        "container_name": "your_container_name",
        "opsview_system_id": "opsview_system_id",
        "directory": "/var/log/opsview",
        "max_retries": 3,
        "retry_delay": 5,
    }

    try:
        with open(file_path, "w") as file:
            file.write("---\n")
            yaml.dump(example_config, file, default_flow_style=False)
            file.write("...\n")
        print(f"Example config file created at: {file_path}")
        return True
    except IOError as e:
        print(f"Error: Unable to create file at '{file_path}'. {e}")
        return False


def load_config_from_yaml(yaml_file):
    with open(yaml_file, "r") as file:
        return yaml.safe_load(file)


def is_blob_service_available(
    storage_account_name, storage_account_key, container_name
):
    """
    Check connectivity to Azure Blob Storage and differentiate between connection issues
    and authentication errors.
    """
    try:
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net",
            credential=storage_account_key,
        )
        container_client = blob_service_client.get_container_client(
            container=container_name
        )
        # Attempt to get container properties
        container_client.get_container_properties()
        return True
    except ClientAuthenticationError:
        print("Authentication failed: Check the storage account credentials.")
        return False
    except ServiceRequestError:
        print("Network error: Unable to connect to Azure Blob Storage.")
        return False
    except Exception as e:
        print(f"Error connecting to Azure Blob Storage: {e}")
        return False


def rename_file_on_success(original_path, prefix="uploaded_at_"):
    """
    Rename the file to indicate a successful upload.
    """
    try:
        new_name = os.path.join(
            os.path.dirname(original_path),
            f"{prefix}{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{os.path.basename(original_path)}",
        )
        os.rename(original_path, new_name)
        print(f"File renamed to: {new_name}")
        return True
    except Exception as e:
        print(f"Error renaming file {original_path}: {e}")
        return False


def upload_file_to_blob(blob_client, file_path, max_retries=3, retry_delay=5):
    """
    Upload a file to Azure Blob Storage with retries.
    max_retries: Number of retry attempts.
    retry_delay: Delay between retries in seconds.
    """
    retry_count = 0
    while retry_count < max_retries:
        try:
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            return True  # Upload succeeded
        except Exception as e:
            print(
                f"Failed to upload {file_path}. Attempt {retry_count + 1} of {max_retries}. Error: {e}"
            )
            retry_count += 1
            time.sleep(retry_delay)

    print(f"Failed to upload {file_path} after {max_retries} attempts.")
    return False


def upload_files_to_blob(
    storage_account_name,
    storage_account_key,
    container_name,
    opsview_system_id,
    directory,
    max_retries=3,
    retry_delay=5,
):
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net",
            credential=storage_account_key,
        )
        container_client = blob_service_client.get_container_client(
            container=container_name
        )

        # List files to upload
        files_to_upload = glob.glob(f"{directory}/results_export_*.tar.gz")

        for file_path in files_to_upload:
            blob_name = f"{opsview_system_id}/{datetime.datetime.now().strftime('%Y%m%d')}/{os.path.basename(file_path)}"
            blob_client = container_client.get_blob_client(blob_name)

            # Upload file with retries
            if upload_file_to_blob(
                blob_client, file_path, max_retries=max_retries, retry_delay=retry_delay
            ):
                # Rename the file to mark as uploaded
                if rename_file_on_success(file_path):
                    print(f"Uploaded and renamed {file_path} to {blob_name}")
                else:
                    print(f"Uploaded but failed to rename {file_path}")
            else:
                print(f"Skipping {file_path} due to repeated upload failures.")

    except Exception as e:
        print(f"Error uploading files to Azure Blob Storage: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Upload files to Azure Blob Storage.")
    parser.add_argument(
        "--storage_account_name",
        type=str,
        help="Azure Storage Account name",
        required=False,
    )
    parser.add_argument(
        "--storage_account_key",
        type=str,
        help="Azure Storage Account key",
        required=False,
    )
    parser.add_argument(
        "--container_name",
        type=str,
        help="Azure Blob Storage container name",
        required=False,
    )
    parser.add_argument(
        "--opsview_system_id", type=str, help="Opsview System ID", required=False
    )
    parser.add_argument(
        "--directory",
        type=str,
        default="/var/log/opsview",
        help="Directory containing files to upload",
        required=False,
    )
    parser.add_argument(
        "--max_retries",
        type=int,
        default=3,
        help="Maximum number of upload retries",
        required=False,
    )
    parser.add_argument(
        "--retry_delay",
        type=int,
        default=5,
        help="Delay between upload retries in seconds",
        required=False,
    )
    parser.add_argument(
        "--config", type=str, help="Path to YAML config file", required=False
    )
    parser.add_argument(
        "--create_example_config",
        type=str,
        help="Create an example YAML config file at the specified location and exit",
        required=False,
    )

    args = parser.parse_args()

    if args.create_example_config:
        success = create_example_config_file(args.create_example_config)
        return 0 if success else 1

    # Load from YAML config if specified
    if args.config:
        config = load_config_from_yaml(args.config)
        storage_account_name = config["storage_account_name"]
        storage_account_key = config["storage_account_key"]
        container_name = config["container_name"]
        opsview_system_id = config["opsview_system_id"]
        directory = config.get(
            "directory", "/var/log/opsview"
        )  # Default to '/var/log/opsview' if not specified
        max_retries = config.get("max_retries", 3)
        retry_delay = config.get("retry_delay", 5)
    else:
        storage_account_name = args.storage_account_name
        storage_account_key = args.storage_account_key
        container_name = args.container_name
        opsview_system_id = args.opsview_system_id
        directory = args.directory
        max_retries = args.max_retries
        retry_delay = args.retry_delay

    # Validation
    required_params = [
        storage_account_name,
        storage_account_key,
        container_name,
        opsview_system_id,
    ]
    if any(param is None or param == "" for param in required_params):
        print(
            "Error: Missing required parameters. Ensure all parameters are provided either via command line or config file."
        )
        return 1

    # Check for Azure Blob Storage connectivity
    if not is_blob_service_available(
        storage_account_name, storage_account_key, container_name
    ):
        print("Unable to connect to Azure Blob Storage. Exiting.")
        return 1

    success = upload_files_to_blob(
        storage_account_name,
        storage_account_key,
        container_name,
        opsview_system_id,
        directory,
        max_retries=max_retries,
        retry_delay=retry_delay,
    )

    if not success:
        return 1

    return 0


if __name__ == "__main__":
    main()
