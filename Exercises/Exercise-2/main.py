import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd


def main():
    BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    TARGET_MODIFIED_TIME = "2024-01-19 10:27"
    
    # Step 1: Get the webpage content
    response = requests.get(BASE_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    # Step 2: Parse table rows to find matching timestamp
    rows = soup.find_all('tr')
    target_file = None
    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= 2:
            modified_time = cols[1].text.strip()
            if modified_time == TARGET_MODIFIED_TIME:
                link = cols[0].find('a')
                if link:
                    target_file = link.get('href')
                    break

    if not target_file:
        print("File not found with specified Last Modified time.")
        return

    # Step 3: Build file URL and download
    file_url = urljoin(BASE_URL, target_file)
    local_filename = "downloaded_weather.csv"
    file_response = requests.get(file_url)
    file_response.raise_for_status()
    with open(local_filename, 'wb') as f:
        f.write(file_response.content)

    # Step 4: Load CSV and find max temperature records
    df = pd.read_csv(local_filename)
    if 'HourlyDryBulbTemperature' not in df.columns:
        print("Column 'HourlyDryBulbTemperature' not found in CSV.")
        return

    max_temp = df['HourlyDryBulbTemperature'].max()
    hottest_records = df[df['HourlyDryBulbTemperature'] == max_temp]
    print("Record(s) with highest HourlyDryBulbTemperature:")
    print(hottest_records)


if __name__ == "__main__":
    main()

