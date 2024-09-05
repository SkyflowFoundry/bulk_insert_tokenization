import time
import requests
import argparse
import concurrent.futures
import tempfile
import os
import logging
import psycopg2
import csv
from tqdm import tqdm
from configparser import ConfigParser
from skyflow.service_account import generate_bearer_token

# Function to generate a PostgreSQL config file template
def generate_pgsql_config_file(config_file):
    with open(config_file, 'w') as file:
        file.write("[DEFAULT]\n")
        file.write("# Skyflow configuration parameters\n\n")
        file.write("skyflow_account_id=\n")
        file.write("vault_url=\n")
        file.write("vault_id=\n")
        file.write("table_name=\n")
        file.write("path_for_credentials_json_file=\n")
        file.write("api_bearer_token=\n")
        file.write("rows_per_chunk=25\n")
        file.write("max_parallel_tasks=5\n")
        file.write("max_calls_per_minute=70\n\n")
        file.write("[INPUT_PGSQL]\n")
        file.write("input_pg_host=\n")
        file.write("input_pg_dbname=\n")
        file.write("input_pg_user=\n")
        file.write("input_pg_password=\n")
        file.write("input_pg_table=\n")
        file.write("input_pg_port=5432\n\n")
        file.write("[OUTPUT_PGSQL]\n")
        file.write("output_pg_host=\n")
        file.write("output_pg_dbname=\n")
        file.write("output_pg_user=\n")
        file.write("output_pg_password=\n")
        file.write("output_pg_table=\n")
        file.write("output_pg_port=5432\n\n")
        file.write("[COLUMN_SKIP]\n")
        file.write("skip_columns=\n")
        file.write("write_skip_columns_as_is=False\n\n")

# Function to load configuration from a file
def load_config(config_file):
    config = ConfigParser()
    config.read(config_file)
    return config

# Function to get filtered columns for the SELECT query
def get_filtered_columns(config, conn, table_name):
    skip_columns = config.get('COLUMN_SKIP', 'skip_columns', fallback='').split(',')
    skip_columns = [col.strip().lower() for col in skip_columns if col.strip()]
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';"
    with conn.cursor() as cursor:
        cursor.execute(query)
        all_columns = [row[0].lower() for row in cursor.fetchall()]
    filtered_columns = [col for col in all_columns if col.lower() not in skip_columns]
    
    print(f"Fetched columns from PostgreSQL: {all_columns}")
    print(f"Columns to skip: {skip_columns}")
    print(f"Filtered columns: {filtered_columns}")
    
    return filtered_columns, skip_columns, all_columns

# Function to connect to PostgreSQL and fetch data
def fetch_pg_data(config, section, offset, rows_per_chunk, filtered_columns, all_columns, skip_columns):
    conn = psycopg2.connect(
        host=config.get(section, 'input_pg_host'),
        dbname=config.get(section, 'input_pg_dbname'),
        user=config.get(section, 'input_pg_user'),
        password=config.get(section, 'input_pg_password'),
        port=config.get(section, 'input_pg_port', fallback='5432')
    )
    table_name = config.get(section, 'input_pg_table')
    columns_str = ', '.join(all_columns[1:])  # Exclude 'skyflow_id'
    query = f"SELECT {columns_str} FROM {table_name} LIMIT {rows_per_chunk} OFFSET {offset};"

    with conn.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
    
    conn.close()

    return data, all_columns[1:]  # Return all columns except 'skyflow_id'

# Function to get row count from PostgreSQL table
def get_pg_row_count(config, section):
    print("Fetching row count from PostgreSQL table")
    conn = psycopg2.connect(
        host=config.get(section, 'input_pg_host'),
        dbname=config.get(section, 'input_pg_dbname'),
        user=config.get(section, 'input_pg_user'),
        password=config.get(section, 'input_pg_password'),
        port=config.get(section, 'input_pg_port', fallback='5432')
    )
    table_name = config.get(section, 'input_pg_table')
    query = f"SELECT COUNT(*) FROM {table_name};"
    with conn.cursor() as cursor:
        cursor.execute(query)
        row_count = cursor.fetchone()[0]
    conn.close()
    print(f"Row count: {row_count}")
    return row_count

# Function to insert tokenized data into PostgreSQL
def insert_pg_data(config, section, data, all_columns, pbar):
    print(f"Inserting tokenized data into PostgreSQL table {config.get(section, 'output_pg_table')}")
    conn = psycopg2.connect(
        host=config.get(section, 'output_pg_host', fallback=config.get('INPUT_PGSQL', 'input_pg_host')),
        dbname=config.get(section, 'output_pg_dbname', fallback=config.get('INPUT_PGSQL', 'input_pg_dbname')),
        user=config.get(section, 'output_pg_user', fallback=config.get('INPUT_PGSQL', 'input_pg_user')),
        password=config.get(section, 'output_pg_password', fallback=config.get('INPUT_PGSQL', 'input_pg_password')),
        port=config.get(section, 'output_pg_port', fallback=config.get('INPUT_PGSQL', 'input_pg_port', fallback='5432'))
    )
    table_name = config.get(section, 'output_pg_table')

    placeholders = ', '.join(['%s'] * len(all_columns))
    query = f"INSERT INTO {table_name} ({', '.join(all_columns)}) VALUES ({placeholders});"
    
    logging.debug(f"All columns: {all_columns}")
    logging.debug(f"Number of columns in table: {len(all_columns)}")

    with conn.cursor() as cursor:
        for row in data:
            logging.debug(f"Row to insert: {row}")
            logging.debug(f"Number of values in row: {len(row)}")
            if len(row) != len(all_columns):
                logging.error(f"Mismatch in column count. Expected {len(all_columns)}, got {len(row)}. Skipping this row.")
                continue
            try:
                cursor.execute(query, row)
            except Exception as e:
                logging.error(f"Failed to insert row: {row}. Error: {e}")
            finally:
                pbar.update(1)
    conn.commit()
    conn.close()

# Function to set up logging
def setup_logging(log_level):
    log_file = 'error.log'
    logging.basicConfig(filename=log_file, level=log_level)
    print(f"Logging set up. Errors will be logged to {log_file}")

# Function to generate bearer token
def token_provider(config):
    bearerToken = ''
    tokenType = ''
    path_to_credentials_json = config.get('DEFAULT', 'path_for_credentials_json_file', fallback=None)
    bearerToken = config.get('DEFAULT', 'api_bearer_token', fallback=None)
    if bearerToken:
        tokenType = "Bearer"
    elif path_to_credentials_json:
        bearerToken, tokenType = generate_bearer_token(path_to_credentials_json)
    else:
        raise ValueError("Either 'api_bearer_token' or 'path_for_credentials_json_file' must be provided in the configuration.")
    print("Bearer token generated successfully")
    return bearerToken, tokenType

# Function to make API call with retries and debug output
def make_api_call(payload, headers, api_url):
    payload["tokenization"] = True
    retries = 3
    for attempt in range(retries):
        response = requests.post(api_url, json=payload, headers=headers)
        if response.status_code == 200:
            return response
        elif response.status_code == 429:
            logging.error(f"Attempt {attempt + 1} failed with status code {response.status_code}: {response.text}. Retrying after backoff.")
            time.sleep(5)
        else:
            logging.error(f"Attempt {attempt + 1} failed with status code {response.status_code}: {response.text}")
            time.sleep(1)
    return response

# Function to process a chunk of rows
def process_chunk(rows, headers, columns, api_url, write_skip_columns_as_is, skip_columns, all_columns):
    tokenized_rows = []

    payload = {
        "quorum": False,
        "records": []
    }

    for row in rows:
        record = {"fields": {}}
        for i, col in enumerate(all_columns[1:]):  # Skip 'skyflow_id'
            if col not in skip_columns:
                record["fields"][col] = row[i]
        payload["records"].append(record)

    response = make_api_call(payload, headers, api_url)
    logging.debug(f"API call payload: {payload}")
    logging.debug(f"API response: {response.status_code} - {response.text}")

    if response.status_code == 200:
        response_data = response.json()
        if 'records' in response_data and all('tokens' in record for record in response_data['records']):
            for i, row in enumerate(rows):
                skyflow_id = response_data['records'][i].get('skyflow_id', None)
                tokenized_row = [skyflow_id]
                for col in all_columns[1:]:  # Skip 'skyflow_id' as it's already added
                    if col in skip_columns:
                        tokenized_row.append(row[all_columns[1:].index(col)])
                    else:
                        tokenized_row.append(response_data['records'][i]['tokens'].get(col, row[all_columns[1:].index(col)]))
                tokenized_rows.append(tokenized_row)
        else:
            logging.error(f"Unexpected response structure: {response_data}")
    else:
        logging.error(f"Failed API call with status {response.status_code}: {response.text}")

    logging.debug(f"Processed chunk. Number of rows: {len(tokenized_rows)}, Columns per row: {len(tokenized_rows[0]) if tokenized_rows else 0}")
    return tokenized_rows

# Main script execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Insert data into Skyflow vault and write tokens to output PostgreSQL.")
    parser.add_argument('--gen-config-pgsql', action='store_true', help='Generate a config file template for PostgreSQL. No other options are accepted with this.')
    parser.add_argument('--config-file', help='Path to the configuration file.')
    parser.add_argument('--log-level', help='Set the logging level', default='ERROR')

    args = parser.parse_args()

    if args.gen_config_pgsql:
        generate_pgsql_config_file('config_pgsql.ini')
        exit(0)

    if not args.config_file:
        parser.error("The --config-file option is required if not generating a config file.")
    
    config = load_config(args.config_file)

    log_level = getattr(logging, args.log_level.upper(), logging.ERROR)
    setup_logging(log_level)

    rows_per_chunk = int(config.get('DEFAULT', 'rows_per_chunk', fallback='25'))
    max_parallel_tasks = int(config.get('DEFAULT', 'max_parallel_tasks', fallback='5'))
    write_skip_columns_as_is = config.getboolean('COLUMN_SKIP', 'write_skip_columns_as_is', fallback=False)

    accessToken, tokenType = token_provider(config)
    headers = {
        "X-SKYFLOW-ACCOUNT-ID": config.get('DEFAULT', 'skyflow_account_id'),
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"{tokenType} {accessToken}"
    }

    api_url = f"https://{config.get('DEFAULT', 'vault_url')}/v1/vaults/{config.get('DEFAULT', 'vault_id')}/{config.get('DEFAULT', 'table_name')}"

if 'INPUT_PGSQL' in config and 'OUTPUT_PGSQL' in config:
    row_count = get_pg_row_count(config, 'INPUT_PGSQL')
    print(f"Starting processing of {row_count} rows")

    conn = psycopg2.connect(
        host=config.get('INPUT_PGSQL', 'input_pg_host'),
        dbname=config.get('INPUT_PGSQL', 'input_pg_dbname'),
        user=config.get('INPUT_PGSQL', 'input_pg_user'),
        password=config.get('INPUT_PGSQL', 'input_pg_password'),
        port=config.get('INPUT_PGSQL', 'input_pg_port', fallback='5432')
    )

    filtered_columns, skip_columns, all_columns = get_filtered_columns(config, conn, config.get('INPUT_PGSQL', 'input_pg_table'))
    all_columns = ['skyflow_id'] + all_columns  # Add skyflow_id to the beginning of all_columns

    with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='') as temp_output_file:
        temp_output_path = temp_output_file.name

    with tqdm(total=row_count, desc="Tokenizing and writing to temp file", unit="rows") as pbar_tokenizing:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_tasks) as executor:
            futures = []
            for offset in range(0, row_count, rows_per_chunk):
                input_data, _ = fetch_pg_data(config, 'INPUT_PGSQL', offset, rows_per_chunk, filtered_columns, all_columns, skip_columns)
                future = executor.submit(process_chunk, input_data, headers, filtered_columns, api_url, write_skip_columns_as_is, skip_columns, all_columns)
                futures.append(future)

                if len(futures) >= max_parallel_tasks or offset + rows_per_chunk >= row_count:
                    for completed_future in concurrent.futures.as_completed(futures):
                        result = completed_future.result()
                        with open(temp_output_path, mode='a', newline='') as temp_output_file:
                            writer = csv.writer(temp_output_file)
                            writer.writerows(result)
                        pbar_tokenizing.update(len(result))
                    futures = []

    with open(temp_output_path, mode='r') as temp_output_file:
        reader = csv.reader(temp_output_file)
        tokenized_data = list(reader)

    with tqdm(total=len(tokenized_data), desc="Writing to destination PostgreSQL", unit="rows") as pbar_writing:
        insert_pg_data(config, 'OUTPUT_PGSQL', tokenized_data, all_columns, pbar_writing)

    os.remove(temp_output_path)
    print("Processing completed successfully")
