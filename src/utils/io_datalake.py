import os
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

def upload_to_datalake(local_path: str, remote_path: str):
    service_client = DataLakeServiceClient(
        account_url=f"https://{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net",
        credential=os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    )
    file_system_client = service_client.get_file_system_client(file_system=os.getenv("AZURE_FILE_SYSTEM"))
    file_client = file_system_client.get_file_client(remote_path)

    with open(local_path, "rb") as f:
        file_client.upload_data(f, overwrite=True)

    print(f"Uploaded {local_path} -> {remote_path} (Azure Data Lake)")
