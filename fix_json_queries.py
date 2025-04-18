import os
import json

def sanitize_string(s):
    """Escape problematic characters in any string."""
    if not isinstance(s, str):
        return s
    s = s.replace('\\', '\\\\')
    s = s.replace('"', '\\"')
    s = s.replace('\n', '\\n')
    return s

def sanitize_json(obj):
    """Recursively sanitize all strings in the JSON object."""
    if isinstance(obj, dict):
        return {k: sanitize_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_json(elem) for elem in obj]
    elif isinstance(obj, str):
        return sanitize_string(obj)
    else:
        return obj

def process_folder(folder_path, backup=True):
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            filepath = os.path.join(folder_path, filename)
            try:
                with open(filepath, "r", encoding="utf-8-sig") as f:
                    raw_text = f.read()
                # First: Try normal JSON load
                data = json.loads(raw_text)
            except json.JSONDecodeError as e:
                print(f"Trying to sanitize: {filename} -> {e}")
                # Fallback: manually fix bad characters
                raw_text = raw_text.replace('\r', '').replace('\n', '\\n')
                raw_text = raw_text.replace('"', '\\"')
                raw_text = raw_text.replace('\\\\"', '\\"')  # Fix double escaping
                try:
                    data = json.loads(raw_text)
                except Exception as e2:
                    print(f"Still broken: {filename} -> {e2}")
                    continue
            
            # Sanitize properly
            cleaned = sanitize_json(data)

            output_path = filepath
            if backup:
                output_path = filepath.replace(".json", "_fixed.json")

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(cleaned, f, indent=2)

            print(f"Fixed: {filename} -> {output_path}")

if __name__ == "__main__":
    folder = "./conformed"  # Adjust your folder
    process_folder(folder, backup=True)
