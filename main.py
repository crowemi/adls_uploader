import adls_uploader as ad
import datetime

def main():

    print('Main: Start.')


    uploader = ad.AdlsUploader(f"E:\\code\\adls_uploader", "configuration.json")

    #
    uploader.create_container(None)
    uploader.upload_file()

    print('Main: End.')

if __name__ == "__main__":
    main()