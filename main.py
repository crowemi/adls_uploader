import adls_uploader as ad

def main():

    print('Main: Start.')

    uploader = ad.AdlsUploader(f"E:\\code\\adls_uploader", "configuration.json")
    uploader.create_container(None)

    

    print('Main: End.')

if __name__ == "__main__":
    main()