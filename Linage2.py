import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
import json
import os

def extract_table_aliases(parsed):
    """Extract mapping of alias -> real table name from FROM and JOIN."""
    table_alias_map = {}
    from_seen = False
    for token in parsed.tokens:
        if token.is_group:
            table_alias_map.update(extract_table_aliases(token))
        if token.ttype is Keyword and token.value.upper() in ('FROM', 'JOIN'):
            from_seen = True
        elif from_seen:
            if isinstance(token, Identifier):
                real_table = token.get_real_name()  # table
                alias = token.get_alias() or real_table  # alias or table name
                table_alias_map[alias] = real_table
                from_seen = False
            elif isinstance(token, IdentifierList):
                for subtoken in token.get_identifiers():
                    real_table = subtoken.get_real_name()
                    alias = subtoken.get_alias() or real_table
                    table_alias_map[alias] = real_table
                from_seen = False
    return table_alias_map

def extract_column_sources(parsed, table_alias_map):
    """Extract column names and their source tables."""
    column_sources = []
    select_seen = False
    for token in parsed.tokens:
        if token.is_group:
            column_sources.extend(extract_column_sources(token, table_alias_map))
        if token.ttype is DML and token.value.upper() == 'SELECT':
            select_seen = True
        elif select_seen:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    col_name = identifier.get_real_name()
                    parent_name = identifier.get_parent_name()  # alias (or table)
                    actual_table = table_alias_map.get(parent_name, parent_name) if parent_name else None
                    column_sources.append((col_name, actual_table))
                select_seen = False
            elif isinstance(token, Identifier):
                col_name = token.get_real_name()
                parent_name = token.get_parent_name()
                actual_table = table_alias_map.get(parent_name, parent_name) if parent_name else None
                column_sources.append((col_name, actual_table))
                select_seen = False
    return column_sources

def parse_sql_and_extract_lineage(sql_text):
    parsed = sqlparse.parse(sql_text)[0]
    table_alias_map = extract_table_aliases(parsed)
    column_sources = extract_column_sources(parsed, table_alias_map)
    return column_sources

def parse_json_file(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    lineage_results = []

    # Now assuming structure as you uploaded — applications -> jobs -> tags -> flows
    for app in data.get('applications', []):
        for job in app.get('jobs', []):
            for tag in job.get('tags', []):
                # Handle flows
                for flow in tag.get('flows', []):
                    query = flow.get('selectExpr')
                    if query:
                        try:
                            lineage = parse_sql_and_extract_lineage(query)
                            lineage_results.append((flow.get('registerAs'), lineage))
                        except Exception as e:
                            print(f"Error parsing query: {e}")

                # Handle ingestion if present
                for ingestion in tag.get('ingestion', []):
                    query = ingestion.get('selectExpr')
                    if query:
                        try:
                            lineage = parse_sql_and_extract_lineage(query)
                            lineage_results.append((ingestion.get('registerAs'), lineage))
                        except Exception as e:
                            print(f"Error parsing ingestion query: {e}")

    return lineage_results

def write_lineage_to_text(lineage_results, output_file):
    with open(output_file, 'w', encoding='utf-8') as f:
        for table_name, lineage in lineage_results:
            for column_name, source_table in lineage:
                if source_table:
                    f.write(f"Column `{column_name}` in table `{table_name}` is derived from table `{source_table}`\n")
                else:
                    f.write(f"Column `{column_name}` in table `{table_name}` has no specific source table (literal or expression)\n")

# ---- Main Runner ----
if __name__ == "__main__":
    input_folder = "/path/to/your/json/folder"  # change this
    output_file = "final_lineage_output.txt"

    all_lineages = []
    for filename in os.listdir(input_folder):
        if filename.endswith('.json'):
            json_path = os.path.join(input_folder, filename)
            file_lineages = parse_json_file(json_path)
            all_lineages.extend(file_lineages)

    write_lineage_to_text(all_lineages, output_file)
    print(f"Lineage extraction completed. Output saved to {output_file}")
