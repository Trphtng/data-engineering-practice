import boto3
import gzip
import os


def main():
    s3 = boto3.client('s3')

    bucket_name = 'commoncrawl'
    gz_key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
    local_gz_path = 'wet.paths.gz'

    print("Tải file danh sách WET paths...")
    s3.download_file(bucket_name, gz_key, local_gz_path)

    print("Đọc dòng đầu tiên trong file wet.paths.gz...")
    with gzip.open(local_gz_path, 'rt') as f:
        first_wet_uri = f.readline().strip()

    print(f"Đường dẫn đến file WET đầu tiên: {first_wet_uri}")

    local_wet_filename = os.path.basename(first_wet_uri)
    print(f"Tải file WET thực tế: {first_wet_uri}...")
    s3.download_file(bucket_name, first_wet_uri, local_wet_filename)

    print(f"Đọc nội dung từ file {local_wet_filename}:\n")
    with gzip.open(local_wet_filename, 'rt', encoding='utf-8', errors='ignore') as f:
        for line in f:
            print(line.strip())


if __name__ == "__main__":
    main()
