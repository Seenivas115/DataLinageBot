import os
import re
import json

def strong_fix_query(text):
    """Fix both newlines and unescaped quotes inside 'query' fields."""
    def replacer(match):
        content = match.group(1)
        # Escape backslashes first
        content = content.replace('\\', '\\\\')
        # Escape double quotes
        content = content.replace('"', '\\"')
        # Escape real newlines
        content = content.replace('\n', '\\n')
        return f'"query": "{content}"'

    # regex to match "query": "...."
    pattern = r'"query"\s*:\s*"((?:[^"\\]|\\.)*?)"'
    fixed_text = re.sub(pattern, replacer, text, flags=re.DOTALL)
    return fixed_text

def process_folder(folder_path, backup=True):
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            filepath = os.path.join(folder_path, filename)
            with open(filepath, "r", encoding="utf-8-sig") as f:
                raw_text = f.read()

            fixed_text = strong_fix_query(raw_text)

            try:
                data = json.loads(fixed_text)
            except json.JSONDecodeError as e:
                print(f"Still invalid: {filename} -> {e}")
                continue

            output_path = filepath
            if backup:
                output_path = filepath.replace(".json", "_fixed.json")

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            print(f"Fixed: {filename} -> {output_path}")

if __name__ == "__main__":
    folder = "./conformed"  # <-- adjust your folder
    process_folder(folder, backup=True)
