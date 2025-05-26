import logging
from urllib.parse import urlparse
import os
import requests
import logging
import pandas as pd # type: ignore
from bs4 import BeautifulSoup # type: ignore 

DATA_DIR = 'DATA'
HOME_URL = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
FILE_DATE =  '2024-01-19 15:45'
FILE_METRIC = '2.5M'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

def fetch_url(url: str) -> BeautifulSoup:
    """Verifies working URL and creates soup instance ready for scraping."""
    response = requests.get(url)
    response.raise_for_status()
    logging.info(f'Successfully fetched: {url}. Status: {response.status_code}')
    return BeautifulSoup(response.content, 'html.parser')

def download_csv(file_url: str, save_path: str) -> str:
    """Downloads a CSV file from a given URL and saves it locally."""    
    os.makedirs(DATA_DIR, exist_ok=True)
    filename = os.path.basename(urlparse(file_url).path)
    with open(save_path, 'wb') as f:
        f.write(requests.get(file_url).content)
    logging.info(f'Saved file: {filename} locally.')
    return save_path

def fetching_csv_data(csv_file: str) -> str:
    """Seperated pandas logic in case we want to fetch further data from our csv."""
    df = pd.read_csv(csv_file)                
    df['HourlyDryBulbTemperature'] = pd.to_numeric(df['HourlyDryBulbTemperature'], errors='coerce')
    logging.info(df['HourlyDryBulbTemperature'].nlargest(3))


def main():
    try:
        landing_page_content_soup = fetch_url(HOME_URL)

        landing_page_first_table_row = landing_page_content_soup.find('tr')
        hyper_links_from_first_table_row = landing_page_first_table_row.find_all('a')
        last_modified_hyperlink = None

        for hyperlink in hyper_links_from_first_table_row:
            if hyperlink.text.strip() == 'Last modified':
                last_modified_hyperlink = hyperlink['href']

        
        last_modified_url = f'{HOME_URL}{last_modified_hyperlink}'

        last_modified_page_content_soup = fetch_url(last_modified_url)

        all_table_rows_last_modified_page = last_modified_page_content_soup.find_all('tr')
        target_csv_file_hyperlink = None

        for table_row in all_table_rows_last_modified_page:
            all_table_data_cells = table_row.find_all('td')

            if len(all_table_data_cells) < 2: 
                continue

            table_data_cell_file_date = all_table_data_cells[1].text.strip()
            table_data_cell_file_metric = all_table_data_cells[2].text.strip()

            if FILE_DATE == table_data_cell_file_date and FILE_METRIC == table_data_cell_file_metric:
                target_csv_file_hyperlink = all_table_data_cells[0].find('a')['href']
                logging.info(f'Found target CSV file: {target_csv_file_hyperlink}.')
                break
        
        csv_file_download_url = f'{HOME_URL}{target_csv_file_hyperlink}'
        local_save_path = os.path.join(DATA_DIR, os.path.basename(target_csv_file_hyperlink))
        
        downloaded_csv_path = download_csv(csv_file_download_url, local_save_path)
        fetching_csv_data(downloaded_csv_path)

    except Exception as e:
        logging.error(f'Error processing: {e}.')

if __name__ == "__main__":
    main()
