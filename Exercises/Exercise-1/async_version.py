'''
A lot to unpack here, but let's use the shopping analogy. Typically when shopping, we
do this in a linear fashion. We have a list and we can from point A to B and so on.
However by using async in Python, we can actually get items from our list
simultaneously. Its like having a designated friend for each item in the list,
and then meeting up after. Async is best used for 'waiting tasks'. In this case since
downloading and extracting files form zips isn't computation heavy, we can easily switch from
one task to another, however for others that require more computing power, this isn't the
case since we are working really hard on one task, we aren't able to juggle the other tasks
hence it becomes a linear process which is not purpose of async.
'''

import os
import zipfile
from urllib.parse import urlparse
import aiofiles # type: ignore
import aiohttp # type: ignore
import asyncio

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

directory_name = 'downloads_async'

async def download_and_extract(session, uri):
    try:
        filename = os.path.basename(urlparse(uri).path)
        zip_path = os.path.join(directory_name, filename)

        async with session.get(uri) as response:
            if response.status != 200:
                print(f'Failed to download {uri}, status: {response.status}.')
                return

            # Save ZIP file to disk using aiofiles
            async with aiofiles.open(zip_path, 'wb') as f:
                await f.write(await response.read())
            print(f'Downloaded {filename}.')

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.endswith('.csv'):
                    zip_ref.extract(file_info, directory_name)
                    print(f'Extracted CSV: {file_info.filename}.')
                    break
        
        os.remove(zip_path)
        print(f'Deleted ZIP: {filename}.')
    
    except Exception as e:
        print(f'Error processing URI: {e}.') 

async def main():
    os.makedirs(directory_name, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        tasks = [download_and_extract(session, uri) for uri in download_uris]
        await asyncio.gather(*tasks)
    
if __name__ == "__main__":
    asyncio.run(main())
