import os
import requests
import zipfile
from pathlib import Path

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip" 
]

DOWNLOAD_DIR = Path("downloads")


def create_download_dir():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


def get_filename_from_url(url: str) -> str:
    return url.split("/")[-1]


def download_file(url: str, dest_path: Path) -> bool:
    try:
        print(f"Downloading: {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            f.write(response.content)
        print(f"Saved to: {dest_path}")
        return True
    except requests.RequestException as e:
        print(f"Failed to download {url}: {e}")
        return False


def extract_zip(zip_path: Path, extract_to: Path):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"Extracted: {zip_path.name}")
        zip_path.unlink()  # Delete the zip file after extraction
    except zipfile.BadZipFile as e:
        print(f"Error extracting {zip_path.name}: {e}")


def main():
    create_download_dir()
    
    for url in download_uris:
        filename = get_filename_from_url(url)
        zip_path = DOWNLOAD_DIR / filename
        
        if download_file(url, zip_path):
            extract_zip(zip_path, DOWNLOAD_DIR)


if __name__ == "__main__":
    main()
