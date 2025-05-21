import requests
import os
import zipfile
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor

from urllib.parse import urlparse

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

directory_name = 'downloads_threads'

def download_and_extract(uri):
    try:
        filename = os.path.basename(urlparse(uri).path)
        zip_path = os.path.join(directory_name, filename)

        # Download the file.
        response = requests.get(uri)
        if response.status_code != 200:
            print(f'Failed to download {uri}, status code: {response.status_code}.')
            return

        with open(zip_path, 'wb') as f:
            f.write(response.content)
        print(f'Downloaded {filename} successfully.')

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.endswith('.csv'):
                    print(f'Extracted: {file_info.filename} from {filename}.')
                    zip_ref.extract(file_info, directory_name)
                    break
        
        os.remove(zip_path)
        print(f'Deleted ZIP: {filename}.')

    except Exception as e:
        print(f'Error processing: {uri}.')
    
                    
def main():
    os.makedirs(directory_name, exist_ok=True)
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(download_and_extract, download_uris)

    
if __name__ == "__main__":
    main()
