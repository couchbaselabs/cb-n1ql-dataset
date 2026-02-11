import os
import re
import json
import csv
from pathlib import Path

def convert_sqlite_to_mysql_type(sqlite_type):
    sqlite_type = sqlite_type.upper().strip()
    mapping = {
        'INTEGER': 'INT', 'INT64': 'BIGINT', 'REAL': 'DOUBLE', 'FLOAT64': 'DOUBLE',
        'TEXT': 'TEXT', 'STRING': 'VARCHAR(255)', 'BOOLEAN': 'TINYINT(1)', 'BOOL': 'TINYINT(1)',
    }
    for k, v in mapping.items():
        if sqlite_type.startswith(k):
            return v
    return sqlite_type

def convert_ddl_to_mysql(ddl):
    ddl = ddl.replace('AUTOINCREMENT', 'AUTO_INCREMENT').replace('"', '`')
    for t in ['INT64', 'FLOAT64', 'STRING', 'INTEGER', 'REAL', 'BOOLEAN']:
        ddl = re.sub(rf'\b{t}\b', convert_sqlite_to_mysql_type(t), ddl, flags=re.IGNORECASE)
    return ddl.rstrip().rstrip(';') + ' ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;'

def escape_value(v):
    if v is None: return 'NULL'
    if isinstance(v, bool): return '1' if v else '0'
    if isinstance(v, (int, float)): return str(v)
    return "'" + str(v).replace('\\', '\\\\').replace("'", "\\'") + "'"

def main():
    script_dir = Path(__file__).parent
    
    # Read DDL (skip header row, use second column which contains DDL)
    with open(script_dir / 'DDL.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header row
        ddl_statements = [row[1] for row in reader if row and len(row) > 1]
    
    # Load JSON data (handle both root-level arrays and 'sample_rows' structure)
    table_data = {}
    for jf in script_dir.glob('*.json'):
        with open(jf, 'r') as f:
            data = json.load(f)
            # Handle JSON with 'sample_rows' key or direct array
            if isinstance(data, dict) and 'sample_rows' in data:
                table_data[jf.stem.lower()] = data['sample_rows']
            elif isinstance(data, list):
                table_data[jf.stem.lower()] = data
            else:
                table_data[jf.stem.lower()] = []
    
    # Generate dump
    with open(script_dir / 'mysql_dump.sql', 'w') as f:
        f.write("SET FOREIGN_KEY_CHECKS=0;\n\n")
        for ddl in ddl_statements:
            match = re.search(r'CREATE\s+TABLE\s+[`"]?(\w+)[`"]?', ddl, re.IGNORECASE)
            table_name = match.group(1) if match else None
            if table_name:
                f.write(f"DROP TABLE IF EXISTS `{table_name}`;\n")
            f.write(convert_ddl_to_mysql(ddl) + "\n\n")
            if table_name and table_name.lower() in table_data:
                for row in table_data[table_name.lower()]:
                    cols = ', '.join(f'`{c}`' for c in row.keys())
                    vals = ', '.join(escape_value(v) for v in row.values())
                    f.write(f"INSERT INTO `{table_name}` ({cols}) VALUES ({vals});\n")
                f.write("\n")
        f.write("SET FOREIGN_KEY_CHECKS=1;\n")
    print(f"Generated: {script_dir / 'mysql_dump.sql'}")

if __name__ == '__main__':
    main()
