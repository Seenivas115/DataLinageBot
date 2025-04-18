import os
import json

def fix_multiline_query(json_data):
    """Recursively fix 'query' fields in JSON to escape newlines."""
    if isinstance(json_data, dict):
        for key, value in json_data.items():
            if key == "query" and isinstance(value, str):
                json_data[key] = value.replace('\n', '\\n')
            else:
                fix_multiline_query(value)
    elif isinstance(json_data, list):
        for item in json_data:
            fix_multiline_query(item)

def process_folder(folder_path, backup=True):
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            filepath = os.path.join(folder_path, filename)
            with open(filepath, "r", encoding="utf-8-sig") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError as e:
                    print(f"Skipping {filename}: JSON error -> {e}")
                    continue

            fix_multiline_query(data)

            output_path = filepath
            if backup:
                output_path = filepath.replace(".json", "_fixed.json")

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            print(f"Fixed: {filename} -> {output_path}")

if __name__ == "__main__":
    folder = "./conformed"   # <- put your folder path here
    process_folder(folder, backup=True)  # Set backup=False if you want to overwrite
