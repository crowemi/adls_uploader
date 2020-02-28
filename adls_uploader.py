import os, uuid 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

try: 

    ### Configuration 
    container_name = "shpo-integration-application"
    upload_file_path = f"C:/azcopy/logs"

    ###

    print("PyUploader Start.")
    conn = os.getenv("azure_storage_connection_string")
    client = BlobServiceClient.from_connection_string(conn)


    container_list = client.list_containers()
    container_names = [] 

    # get container names
    for container in container_list:
        container_names.append(container.name)

    # check if container exists
    if container_name in container_names:
        print(f"PyUploader: Container {container_name} already exists.")
    else:
        client.create_container("shpo-integration-application")
        print(f"PyUploader: Create container {container_name}")
    
    files_to_upload = os.listdir(upload_file_path)

    for file in files_to_upload:
        with open(os.path.join(upload_file_path, file), "rb") as data: 
            client.get_blob_client(container=container_name, blob=file).upload_blob(data)
            print(f"PyUploader: Uploaded {container_name}/{file}")

    



    print("PyUploader End.")

except Exception as ex: 
    print('Exception:')
    print(ex)