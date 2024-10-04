import time
import requests
import argparse
import concurrent.futures
import tempfile
import os
import logging
import csv
from tqdm import tqdm
from configparser import ConfigParser
from skyflow.service_account import generate_bearer_token

# Function to generate a config file template
def generate_config_file(config_file):
    with open(config_file, 'w') as file:
        file.write("[DEFAULT]\n")
        file.write("# Skyflow configuration parameters\n\n")

        file.write("# Skyflow account ID (Required)\n")
        file.write("skyflow_account_id=\n")

        file.write("# Vault URL (Required: e.g., ebfc9bee4242.vault.skyflowapis.com, do not add https://)\n")
        file.write("vault_url=\n")

        file.write("# Vault ID (Required)\n")
        file.write("vault_id=\n")

        file.write("# Table name in the vault (Required)\n")
        file.write("table_name=\n")

        file.write("# Path to the credentials JSON file (Required if api_bearer_token is not specified)\n")
        file.write("path_for_credentials_json_file=\n")

        file.write("# Bearer token for API calls (Required if path_for_credentials_json_file is not specified)\n")
        file.write("api_bearer_token=\n")

        file.write("# Number of rows to be inserted per API call (Optional: default=25)\n")
        file.write("rows_per_chunk=25\n")

        file.write("# Maximum number of parallel API calls (Optional: default=5, max=7)\n")
        file.write("max_parallel_tasks=5\n")

        file.write("# Maximum API calls per minute (Optional: default=70)\n")
        file.write("max_calls_per_minute=70\n\n")

        file.write("[INPUT_FILE]\n")
        file.write("# Configuration for the input file\n")
        file.write("input_file_path=\n\n")

        file.write("[OUTPUT_FILE]\n")
        file.write("# Configuration for the output file\n")
        file.write("output_file_path=\n\n")

        file.write("[COLUMN_SKIP]\n")
        file.write("# If you want column(s) to be skipped then provide comma seperated column names with no spaces\n")
        file.write("skip_columns=\n")
        file.write("# If you set the below value to True then the skipped columns will be written as is in the destination file\n")
        file.write("write_skip_columns_as_is=False\n\n")

# Function to load configuration from a file
def load_config(config_file):
    config = ConfigParser()
    config.read(config_file)
    return config

# Function to get filtered columns from the file header
def get_filtered_columns(config, rows):
    skip_columns = config.get('COLUMN_SKIP', 'skip_columns', fallback='').split(',')
    skip_columns = [col.strip().lower() for col in skip_columns if col.strip()]
    all_columns = [col.lower() for col in rows[0]]  # Assuming the first row contains the headers
    filtered_columns = [col for col in all_columns if col not in skip_columns]
    
    print(f"Fetched columns from file: {all_columns}")
    print(f"Columns to skip: {skip_columns}")
    print(f"Filtered columns: {filtered_columns}")
    
    return filtered_columns, skip_columns, all_columns

# Function to fetch data from file
def fetch_file_data(rows, offset, rows_per_chunk):
    # Skip the header row by starting from the second row
    return rows[offset:offset+rows_per_chunk], rows[0]  # Ensure the header row is only returned separately

# Function to get row count from file
def get_file_row_count(rows):
    print("Fetching row count from file")
    row_count = len(rows) - 1  # Exclude the header row
    print(f"Row count: {row_count}")
    return row_count

# Function to write tokenized data into file
def write_tokenized_data_to_file(config, tokenized_data, pbar, headers,dialect):
    output_file_path = config.get('OUTPUT_FILE', 'output_file_path')
    print(f"Writing tokenized data to the file {output_file_path}")

    with open(output_file_path, mode='w', newline='', encoding='utf-8') as csvfile:
        
        writer = csv.writer(csvfile,dialect=dialect,escapechar="\\")
# Add the skyflow_id header to the output file
        headers.insert(0,'skyflow_id')
        # Write the header row
        writer.writerow(headers)

        # Write the tokenized data
        writer.writerows(tokenized_data)
        pbar.update(len(tokenized_data))

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
    parser = argparse.ArgumentParser(description="Insert data into Skyflow vault and write tokens to output file.")
    parser.add_argument('--gen-config-csv', action='store_true', help='Generate a config file template. No other options are accepted with this.')
    parser.add_argument('--config-file', help='Path to the configuration file.')
    parser.add_argument('--log-level', help='Set the logging level', default='ERROR')

    args = parser.parse_args()

    if args.gen_config_csv:
        generate_config_file('config_csv.ini')
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

    api_url = f"{config.get('DEFAULT', 'vault_url')}/v1/vaults/{config.get('DEFAULT', 'vault_id')}/{config.get('DEFAULT', 'table_name')}"
    file_dialect = {};

    if 'INPUT_FILE' in config and 'OUTPUT_FILE' in config:
        with open(config.get('INPUT_FILE', 'input_file_path'), mode='r', newline='', encoding='utf-8') as csvfile:
            file_dialect = csv.Sniffer().sniff(csvfile.readline())
            csvfile.seek(0)
            reader = csv.reader(csvfile,  dialect=file_dialect,escapechar="\\")
            rows = list(reader)

        # Extract and store the headers separately
        headers_row = rows[0]
        data_rows = rows[1:]  # Skip the header row
       
        row_count = get_file_row_count(rows)
        print(f"Starting processing of {row_count} rows")

        filtered_columns, skip_columns, all_columns = get_filtered_columns(config, [headers_row] + data_rows)
        all_columns = ['skyflow_id'] + all_columns  # Add skyflow_id to the beginning of all_columns

        with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='') as temp_output_file:
            temp_output_path = temp_output_file.name

        with tqdm(total=row_count, desc="Tokenizing and writing to temp file", unit="rows") as pbar_tokenizing:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_tasks) as executor:
                futures = []
                for offset in range(0, row_count, rows_per_chunk):
                    input_data, _ = fetch_file_data(data_rows, offset, rows_per_chunk)  # Pass data_rows without header
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

        with tqdm(total=len(tokenized_data), desc="Writing to destination file", unit="rows") as pbar_writing:
            # Write the headers first, followed by the tokenized data
            write_tokenized_data_to_file(config, tokenized_data, pbar_writing, headers_row, file_dialect)

        os.remove(temp_output_path)
        print("Processing completed successfully")