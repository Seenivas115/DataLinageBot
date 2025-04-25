import os
import json
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Function
from sqlparse.tokens import DML, Name, Wildcard, Keyword
from collections import defaultdict

# Set up layers and paths
LAYER_ORDER = ["raw", "sanitized", "conformed", "curated"]
LAYER_DIRS = {layer: f"./{layer}" for layer in LAYER_ORDER}


def extract_table_aliases(stmt):
    """Extract table aliases or real names from FROM and JOIN clauses."""
    aliases = {}
    from_seen = False
    for token in stmt.tokens:
        if token.is_group:
            aliases.update(extract_table_aliases(token))
        if token.ttype is Keyword and token.value.upper() in ("FROM", "JOIN"):
            from_seen = True
        elif from_seen:
            if isinstance(token, Identifier):
                real_table = token.get_real_name()
                alias = token.get_alias() or real_table
                aliases[alias] = real_table
                from_seen = False
            elif isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    real_table = identifier.get_real_name()
                    alias = identifier.get_alias() or real_table
                    aliases[alias] = real_table
                from_seen = False
    return aliases


def extract_columns_and_sources(query):
    parsed = sqlparse.parse(query)
    col_map = {}

    for stmt in parsed:
        if stmt.get_type() != "SELECT":
            continue

        alias_map = extract_table_aliases(stmt)
        select_seen = False

        for token in stmt.tokens:
            if token.ttype is DML and token.value.upper() == "SELECT":
                select_seen = True
            elif select_seen:
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        col_name = identifier.get_alias() or identifier.get_real_name()
                        src_table = identifier.get_parent_name()
                        if src_table and src_table in alias_map:
                            col_map[col_name] = [f"{alias_map[src_table]}.{identifier.get_real_name()}"]
                        else:
                            col_map[col_name] = [identifier.get_real_name()]
                elif isinstance(token, Identifier):
                    col_name = token.get_alias() or token.get_real_name()
                    src_table = token.get_parent_name()
                    if src_table and src_table in alias_map:
                        col_map[col_name] = [f"{alias_map[src_table]}.{token.get_real_name()}"]
                    else:
                        col_map[col_name] = [token.get_real_name()]
                elif isinstance(token, Function):
                    col_name = token.get_alias() or token.get_real_name()
                    inputs = [t.value for t in token.tokens if t.ttype in (Name, Wildcard)]
                    col_map[col_name] = inputs
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

            for app in data.get("applications", []):
                for job in app.get("jobs", []):
                    for tag in job.get("tags", []):
                        # Process flows
                        for flow in tag.get("flows", []):
                            query = flow.get("queryFetch") or flow.get("query", "")
                            output_table = flow.get("registerAsName") or flow.get("saveOutputTable", "")
                            if not query or not output_table:
                                continue
                            col_lineage = extract_columns_and_sources(query)
                            for out_col, input_cols in col_lineage.items():
                                out_fq = f"{output_table}.{out_col}"
                                lineage_dict[out_fq] = input_cols
                        # Process ingestion
                        for ingestion in tag.get("ingestion", []):
                            query = ingestion.get("queryFetch") or ingestion.get("query", "")
                            output_table = ingestion.get("registerAsName") or ingestion.get("saveOutputTable", "")
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
