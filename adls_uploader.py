import os, uuid, py_linq, json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

class AdlsUploader:
    
    def __init__( self, config_path, config_file_name ):
        
        print (f'AdlsUploader.Initialize: Start.')
        print (f'AdlsUploader.Initialize: config_path: {config_path}.')
        print (f'AdlsUploader.Initialize: config_file_name: {config_file_name}.')
        
        with open(os.path.join(config_path, config_file_name), "rb") as file:
            try: 
                self.config = json.loads(file.read())
                self.client = BlobServiceClient.from_connection_string(os.getenv("azure_storage_connection_string"))
            except Exception as ex: 
                print(f'AdlsUploader.Initialize: Failed loading {config_path}/{config_file_name}.')

        print (f'AdlsUploader.Initialize: End.')

    def create_container(self, container_name):

        if container_name is None:
            container_name = self.config["adls_settings"]["container"]

        container_list = self.client.list_containers()
        container_names = [] 

        # get container names
        for container in container_list:
            container_names.append(container.name)

        # check if container exists
        if container_name in container_names:
            print(f"adls-uploader: Container {container_name} already exists.")
        else:
            self.client.create_container(container_name)
            print(f"adls-uploader: Create container {container_name}")

        