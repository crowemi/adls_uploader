import os, uuid, py_linq, json, logging, datetime
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

class AdlsUploader:
    
    def __init__( self, config_path, config_file_name ):
        
        print (f'AdlsUploader.__init__: Enter.')
        print (f'AdlsUploader.__init__: config_path: {config_path}.')
        print (f'AdlsUploader.__init__: config_file_name: {config_file_name}.')
        
        with open(os.path.join(config_path, config_file_name), "rb") as file:
            try: 
                self.config = json.loads(file.read())
            except Exception as ex: 
                print(f'AdlsUploader.__init__: Failed initializing adlsloader {config_path}/{config_file_name}.')
                print (f'AdlsUploader.create_contianer: {ex}')

        print (f'AdlsUploader.__init__: Leaving.')

    def create_client(self):

        try:
            return BlobServiceClient.from_connection_string("blob_storage_connection_string")
        except Exception as ex:
            print (f'AdlsUploader.create_client: {ex}')

    def create_container(self, container_name):

        print (f'AdlsUploader.create_contianer: Enter.')

        try: 
            if container_name is None:
                container_name = self.config["adls_settings"]["container"]
            
            client = self.create_client()
            container_list = client.list_containers()
            container_names = [] 

            # get container names
            for container in container_list:
                container_names.append(container.name)

            # check if container exists
            if container_name in container_names:
                print(f"adls-uploader: Container {container_name} already exists.")
            else:
                client.create_container(container_name)
                print(f"adls-uploader: Create container {container_name}")

        except Exception as ex: 
            print (f'AdlsUploader.create_contianer: {ex}')

        
        print (f'AdlsUploader.create_contianer: Leaving.')

    def upload_file(self):
        
        print (f'AdlsUploader.upload_file: Start.')

        client = self.create_client()

        print (f'AdlsUploader.upload_files: path: {self.config["directory"]}')
        print (f'AdlsUploader.upload_files: file_to_folder: {self.config["file_to_folder"]}')

        # get name of all files in directory
        if self.config["directory"] != None: 
            self.file_manifest = os.listdir(self.config["directory"]) 
            print (f'AdlsUploader.upload_files: files added to manifest: {len(self.file_manifest)}.')
        else:
            print (f'AdlsUploader.upload_files: No directory provided in configuration file.')

        if self.config['file_to_folder'] == True:
           for file in self.file_manifest:
                with open(os.path.join(self.config['directory'], file), "rb") as data: 
                    upload_path = file + f"/{file}_{datetime.datetime.now()}"
                    client.get_blob_client(container=self.config['adls_settings']['container'], blob=upload_path).upload_blob(data)
                    print(f"AdlsUploader.upload_files: Uploaded {upload_path}") 
        else: 
            print (f'')

        print (f'AdlsUploader.create_contianer: Start.')

        