import os
import json
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Function
from sqlparse.tokens import DML, Name, Wildcard
from collections import defaultdict

# Set up layers and paths
LAYER_ORDER = ["raw", "sanitized", "conformed", "curated"]
LAYER_DIRS = {layer: f"./{layer}" for layer in LAYER_ORDER}

def extract_columns_and_sources(query):
    parsed = sqlparse.parse(query)
    col_map = {}

    for stmt in parsed:
        if stmt.get_type() != "SELECT":
            continue

        tokens = stmt.tokens
        select_seen = False

        for token in tokens:
            if token.ttype is DML and token.value.upper() == "SELECT":
                select_seen = True
            elif select_seen:
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        output_col = identifier.get_alias() or identifier.get_real_name()
                        input_cols = [t.value for t in identifier.tokens if t.ttype in (Name, Wildcard)]
                        col_map[output_col] = input_cols
                elif isinstance(token, Identifier):
                    output_col = token.get_alias() or token.get_real_name()
                    input_cols = [t.value for t in token.tokens if t.ttype in (Name, Wildcard)]
                    col_map[output_col] = input_cols
                elif isinstance(token, Function):
                    output_col = token.get_alias() or token.get_real_name()
                    input_cols = [t.value for t in token.tokens if t.ttype == Name]
                    col_map[output_col] = input_cols
                break
    return col_map

def parse_all_jsons():
    lineage_dict = {}

    for layer in LAYER_ORDER:
        layer_dir = LAYER_DIRS[layer]
        if not os.path.isdir(layer_dir):
            continue

        for file in os.listdir(layer_dir):
            if not file.endswith(".json"):
                continue

            filepath = os.path.join(layer_dir, file)
            with open(filepath, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    continue

            flows = data.get("flows", {}).get("flow", [])
            for flow in flows:
                query = flow.get("queryFetch") or flow.get("query", "")
                output_table = flow.get("registerAsName") or flow.get("saveOutputTable", "")
                if not query or not output_table:
                    continue

                col_lineage = extract_columns_and_sources(query)
                for out_col, input_cols in col_lineage.items():
                    out_fq = f"{output_table}.{out_col}"
                    lineage_dict[out_fq] = input_cols

    return lineage_dict

def convert_lineage_to_text(lineage_dict):
    lines = []
    for output_col, input_cols in lineage_dict.items():
        col_name = output_col.split(".")[-1]
        table_name = ".".join(output_col.split(".")[:-1])
        inputs = "\n   - ".join(input_cols)
        line = f"Column `{col_name}` in table `{table_name}` is derived from:\n   - {inputs}"
        lines.append(line)
    return "\n\n".join(lines)

def save_lineage_text(lineage_text, filename="lineage_context.txt"):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(lineage_text)

def main():
    lineage_dict = parse_all_jsons()
    lineage_text = convert_lineage_to_text(lineage_dict)
    save_lineage_text(lineage_text)
    print("✅ Lineage extraction complete.")
    print("Saved context to: lineage_context.txt")

if __name__ == "__main__":
    main()
