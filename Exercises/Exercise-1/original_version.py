'''
First attempt of exercise one. Successfully completed via doing the basics.
Other files will now include the extra credit implementation.
'''

import requests
import os
import zipfile
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


def main():

    # Make a directory called 'downloads'.
    directory_name = 'downloads'

    try:
        os.mkdir(directory_name)
        print(f'Directory: {directory_name} created successfully.')
    except FileExistsError:
        print(f'Directory: {directory_name} already exists.')
    except PermissionError:
        print(f'Permissions denied. Unable to create directory: {directory_name}.')
    except Exception as e:
        print(f'An error occured: {e}.')

    # Iterating through download_uris and ensure they have a status 200.
    valid_uris = []
    for uri in download_uris:
        response = requests.get(uri)
        if response.status_code == 200:
            valid_uris.append(uri)
            print(f'URI: {uri} is OK, appended to valid uris.')
        else:
            print(f'URI: {uri} not successful, status: {response.status_code}.')
    print(f'{len(valid_uris)} / {len(download_uris)} are valid for use.')

    # Download OK files and unzip them.
    for uri in valid_uris:
        print(f'Downloading from: {uri}.')

        # Extract filename from the URL.
        filename = os.path.basename(urlparse(uri).path)
        
        try:
            response = requests.get(uri)
            zip_path = os.path.join(directory_name, filename)

            # Writing the content of our uri in binary to ensure no data is changed.
            with open(zip_path, 'wb') as f:
                f.write(response.content)
            print(f'Saved zip to {zip_path}.')

            # Unzip file and extract the first csv.
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for file_info in zip_ref.infolist():
                    if file_info.filename.endswith('.csv'):
                        zip_ref.extract(file_info, directory_name)
                        print(f'Extracted {file_info.filename}.')
                        break

            # Delete the zip.
            os.remove(zip_path)
            print(f'Deleted zip file: {filename}.')
        
        except Exception as e:
            print(f'Failed to process {uri}: {e}.')
        
    print('Extracted all CSV files from valid URIs. Script complete.')
    
        
if __name__ == "__main__":
    main()
