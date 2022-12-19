import os
import subprocess
from azure.storage.filedatalake import DataLakeServiceClient
import ast

def initialize_storage_account(storage_account_name, storage_account_key):
    try:
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)

        return service_client
    except Exception as e:
        print(e)
        raise e

def write_file(file_stream, file_path):
    local_file = open(file_path,'wb')

    local_file.write(file_stream)

    local_file.close()

def download_files_from_directory(service_client, container, cloud_file_path):
    try:
        file_system_client = service_client.get_file_system_client(file_system=container)

        #        local_file = open(local_output,'wb')
        print("New flow")
        file_client = file_system_client.get_file_client(cloud_file_path)

        download = file_client.download_file()

        downloaded_bytes = download.readall()

        return downloaded_bytes
    #        local_file.write(downloaded_bytes)

    #        local_file.close()
    # print("Files Download Completed")
    except Exception as e:
        print(e)
        raise e


def upload_file_to_directory_bulk(service_client, container, cloud_file_path, local_input):
    try:
        file_system_client = service_client.get_file_system_client(file_system=container)

        file_client = file_system_client.get_file_client(cloud_file_path)

        local_file = open(local_input,'r')

        file_contents = local_file.read()

        file_client.upload_data(file_contents, overwrite=True)
        file_client.flush_data(len(file_contents))

    except Exception as e:
        print(e)
        raise e
