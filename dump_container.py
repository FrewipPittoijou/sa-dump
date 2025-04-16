import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Load environment variables from .env file
load_dotenv()

# Get connection string from environment variable
connection_string = os.getenv("CONNECTION_STRING")

# Set the container name and download directory
container_name = "2025-01"
download_dir = "downloaded_blobs"

# Create local directory if it doesn't exist
os.makedirs(download_dir, exist_ok=True)

try:
    # Create the BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get the container client
    container_client = blob_service_client.get_container_client(container_name)

    # List and download all blobs in the container
    print(f"Listing blobs in container: {container_name}")
    for blob in container_client.list_blobs():
        blob_client = container_client.get_blob_client(blob)
        download_file_path = os.path.join(download_dir, blob.name)

        # Ensure subdirectories exist
        os.makedirs(os.path.dirname(download_file_path), exist_ok=True)

        # Download the blob
        with open(download_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

        print(f"Downloaded: {blob.name} → {download_file_path}")

    print("All blobs downloaded successfully.")

except Exception as e:
    print(f"An error occurred: {e}")

