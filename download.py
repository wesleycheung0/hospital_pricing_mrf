import pandas as pd
import requests
import os
from urllib.parse import urlparse

def download_file(url):
    """Download a file from a URL and return the local file name."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Check if the download was successful
        filename = os.path.basename(urlparse(url).path)
        with open(filename, 'wb') as f:
            f.write(response.content)
        return filename
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"Access forbidden for URL: {url}")
        else:
            print(f"HTTP Error for URL {url}: {e}")
        return None
    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None

def process_csv(csv_file, url_column, filename_column):
    """Process a CSV file, download files from URLs and update the CSV with file names."""
    df = pd.read_csv(csv_file)

    for index, row in df.iterrows():
        urls = str(row[url_column]).split('|')
        filenames = []
        for url in urls:
            url = url.strip()
            if url and url != 'nan':  # Check if the URL is not empty
                filename = download_file(url)
                if filename:
                    filenames.append(filename)

        # Update the filename column only if there are downloaded files
        if filenames:
            df.at[index, filename_column] = ' | '.join(filenames)

    df.to_csv(csv_file, index=False)
    print("CSV file updated with downloaded file names.")

# Replace 'your_csv_file.csv' with your CSV file name
# 'url_column_name' with the name of the column containing the URLs
# 'filename_column_name' with the name of the column where you want to store the downloaded file names
process_csv('data/nyc_metadata.csv', 'standard_charge_file_url', 'file_name')
