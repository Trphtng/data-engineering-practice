import glob
import json
import csv
import os

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def convert_json_to_csv(json_file_path):
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if isinstance(data, dict):
        data = [data]  # Convert single dict to list

    flattened = [flatten_json(entry) for entry in data]

    csv_file_path = json_file_path.replace('.json', '.csv')
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=flattened[0].keys())
        writer.writeheader()
        writer.writerows(flattened)

    print(f"✅ Converted: {json_file_path} -> {csv_file_path}")

def main():
    json_files = glob.glob('data/**/*.json', recursive=True)
    if not json_files:
        print("⚠️ No JSON files found in 'data/' directory.")
    for json_file in json_files:
        print(f"🔍 Found: {json_file}")
        convert_json_to_csv(json_file)

if __name__ == "__main__":
    main()
