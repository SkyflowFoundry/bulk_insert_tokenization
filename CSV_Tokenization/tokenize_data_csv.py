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
        file.write("# If you want column(s) to be skipped then provide comma separated column names with no spaces\n")
        file.write("skip_columns=\n")
        file.write("# If you set the below value to True then the skipped columns will be written as is in the destination file\n")
        file.write("write_skip_columns_as_is=False\n\n")

        file.write("[COLUMN_TYPES]\n")
        file.write("# Specify the columns that are of int32 type in the source CSV (comma separated with no spaces)\n")
        file.write("int32_columns=\n\n")

        file.write("[UPSERT]\n")
        file.write("# Specify the column name to be used for upsert operation. This column will be used to update existing records.\n")
        file.write("upsert_column=\n\n")

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
    
    logging.debug(f"Fetched columns from file: {all_columns}")
    logging.debug(f"Columns to skip: {skip_columns}")
    logging.debug(f"Filtered columns: {filtered_columns}")
    
    return filtered_columns, skip_columns, all_columns

# Function to fetch data from file
def fetch_file_data(rows, offset, rows_per_chunk):
    # Return a slice of rows starting from the offset
    return rows[offset:offset+rows_per_chunk]

# Function to get row count from file
def get_file_row_count(rows):
    logging.info("Fetching row count from file")
    row_count = len(rows)  # Data rows only (header is not included)
    logging.info(f"Row count: {row_count}")
    return row_count

# Function to write tokenized data into file
def write_tokenized_data_to_file(config, tokenized_data, pbar, headers_row, dialect):
    output_file_path = config.get('OUTPUT_FILE', 'output_file_path')
    logging.info(f"Writing tokenized data to the file {output_file_path}")

    with open(output_file_path, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, dialect=dialect, escapechar="\\")
        # Add the skyflow_id header to the output file
        headers = ['skyflow_id'] + headers_row
        # Write the header row
        writer.writerow(headers)

        # Write the tokenized data
        for row in tokenized_data:
            writer.writerow(row)
            pbar.update(1)

# Function to set up logging
def setup_logging(log_level):
    log_file = 'error.log'
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Set the logger to the lowest level to capture all logs

    # Remove any existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create file handler which logs messages at specified level or lower
    fh = logging.FileHandler(log_file)

    # Set file handler level
    # If log_level is DEBUG, set file handler to DEBUG, else set to INFO
    if log_level == logging.DEBUG:
        fh.setLevel(logging.DEBUG)
    else:
        fh.setLevel(logging.INFO)

    # Create console handler with appropriate level
    ch = logging.StreamHandler()
    
    # Only show ERROR logs and above by default, but show all logs if DEBUG is specified
    if log_level == logging.DEBUG:
        ch.setLevel(logging.DEBUG)
    else:
        ch.setLevel(logging.ERROR)  # Only show ERROR level logs on console by default

    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    logging.info(f"Logging set up. Errors will be logged to {log_file}")
    if log_level == logging.DEBUG:
        logging.debug("Debug logging enabled on console")

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
    logging.info("Bearer token generated successfully")
    return bearerToken, tokenType

# Function to make API call with retries and debug output
def make_api_call(payload, headers, api_url, config):
    payload["tokenization"] = True
    
    # Add upsert parameter if specified in config
    upsert_column = config.get('UPSERT', 'upsert_column', fallback='')
    if upsert_column.strip():
        payload["upsert"] = upsert_column.strip()
    retries = 3
    for attempt in range(retries):
        try:
            logging.debug("Attempting to make API call")
            response = requests.post(api_url, json=payload, headers=headers)
            logging.debug(f"API call status: {response.status_code}, Response: {response.text}")
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                logging.error(f"Attempt {attempt + 1} failed with status code {response.status_code}: {response.text}. Retrying after backoff.")
                time.sleep(5)
            else:
                logging.error(f"Attempt {attempt + 1} failed with status code {response.status_code}: {response.text}")
                time.sleep(1)
        except Exception as e:
            logging.error(f"Exception during API call attempt {attempt + 1}: {str(e)}")
            logging.debug(f"Exception during API call attempt {attempt + 1}: {str(e)}")
    return response

# Function to process a chunk of rows
def process_chunk(rows, headers, filtered_columns, api_url, write_skip_columns_as_is, skip_columns, all_columns, int32_columns):
    tokenized_rows = []

    payload = {
        "quorum": False,
        "records": []
    }

    try:
        logging.debug("Processing chunk...")
        for row in rows:
            record = {"fields": {}}
            for i, col in enumerate(all_columns):
                if col not in skip_columns:
                    value = row[i]
                    if col in int32_columns:
                        try:
                            value = int(value)
                        except ValueError as e:
                            logging.error(f"Failed to convert column '{col}' to int: {value}")
                            logging.debug(f"Failed to convert column '{col}' to int: {value}")
                    # Clean the column name to ensure no special characters
                    clean_col = col.strip()
                    record["fields"][clean_col] = value
            logging.info(f"Record being added to payload: {record}")
            payload["records"].append(record)
        
        logging.info(f"Full payload being sent to API: {payload}")
        
        response = make_api_call(payload, headers, api_url, config)
        logging.debug(f"API call payload: {payload}")
        logging.debug(f"API response: {response.status_code} - {response.text}")

        if response.status_code == 200:
            response_data = response.json()
            if 'records' in response_data and all('tokens' in record for record in response_data['records']):
                for i, row in enumerate(rows):
                    skyflow_id = response_data['records'][i].get('skyflow_id', None)
                    tokenized_row = [skyflow_id]
                    for idx, col in enumerate(all_columns):
                        if col in skip_columns and write_skip_columns_as_is:
                            tokenized_row.append(row[idx])
                        elif col in skip_columns:
                            tokenized_row.append('')  # Or whatever placeholder you want
                        else:
                            tokenized_value = response_data['records'][i]['tokens'].get(col, row[idx])
                            tokenized_row.append(tokenized_value)
                    tokenized_rows.append(tokenized_row)
            else:
                logging.error(f"Unexpected response structure: {response_data}")
        else:
            logging.error(f"Failed API call with status {response.status_code}: {response.text}")

    except Exception as e:
        logging.error(f"Exception during chunk processing: {str(e)}")
        logging.debug(f"Exception during chunk processing: {str(e)}")

    logging.debug(f"Processed chunk. Number of rows: {len(tokenized_rows)}, Columns per row: {len(tokenized_rows[0]) if tokenized_rows else 0}")
    return tokenized_rows

# Main script execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Insert data into Skyflow vault and write tokens to output file.")
    parser.add_argument('--gen-config-csv', action='store_true', help='Generate a config file template. No other options are accepted with this.')
    parser.add_argument('--config-file', help='Path to the configuration file.')
    parser.add_argument('--log-level', help='Set the logging level for console and error.log file (INFO or DEBUG)', default='INFO')

    args = parser.parse_args()

    if args.gen_config_csv:
        generate_config_file('config_csv.ini')
        exit(0)

    if not args.config_file:
        parser.error("The --config-file option is required if not generating a config file.")
    
    # Map the log level string to a logging level object
    log_level_str = args.log_level.upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    setup_logging(log_level)

    config = load_config(args.config_file)

    rows_per_chunk = int(config.get('DEFAULT', 'rows_per_chunk', fallback='25'))
    max_parallel_tasks = int(config.get('DEFAULT', 'max_parallel_tasks', fallback='5'))
    write_skip_columns_as_is = config.getboolean('COLUMN_SKIP', 'write_skip_columns_as_is', fallback=False)
    int32_columns = config.get('COLUMN_TYPES', 'int32_columns', fallback='').split(',')
    int32_columns = [col.strip().lower() for col in int32_columns if col.strip()]

    accessToken, tokenType = token_provider(config)
    headers = {
        "X-SKYFLOW-ACCOUNT-ID": config.get('DEFAULT', 'skyflow_account_id'),
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"{tokenType} {accessToken}"
    }

    vault_url = config.get('DEFAULT', 'vault_url')
    vault_id = config.get('DEFAULT', 'vault_id')
    table_name = config.get('DEFAULT', 'table_name')

    # Log individual parts of the API URL for verification
    logging.info(f"Vault URL: {vault_url}")
    logging.info(f"Vault ID: {vault_id}")
    logging.info(f"Table Name: {table_name}")

    api_url = f"https://{vault_url}/v1/vaults/{vault_id}/{table_name}"
    logging.info(f"Constructed API URL: {api_url}")
    file_dialect = {}

    if 'INPUT_FILE' in config and 'OUTPUT_FILE' in config:
        input_file_path = config.get('INPUT_FILE', 'input_file_path')
        output_file_path = config.get('OUTPUT_FILE', 'output_file_path')

        logging.info(f"Input file path: {input_file_path}")
        logging.info(f"Output file path: {output_file_path}")

        # Check if the input file exists
        if not os.path.isfile(input_file_path):
            logging.error(f"Input file does not exist at {input_file_path}")
            exit(1)

        # Read a larger sample from the file for the sniffer
        with open(input_file_path, mode='r', newline='', encoding='utf-8') as csvfile:
            # Force the use of excel dialect to avoid potential parsing issues
            file_dialect = csv.excel
            reader = csv.reader(csvfile, dialect=file_dialect)
            rows = list(reader)
            
            # Log the first row to verify headers
            if rows:
                logging.info(f"CSV Headers detected: {rows[0]}")

        # Extract and store the headers separately
        if not rows:
            logging.error("Input file is empty after reading.")
            exit(1)
        headers_row = rows[0]
        data_rows = rows[1:]  # Skip the header row

        if len(data_rows) == 0:
            logging.error("No data rows found in the input file. Exiting.")
            exit(1)
       
        row_count = get_file_row_count(data_rows)
        logging.info(f"Starting processing of {row_count} rows")

        filtered_columns, skip_columns, all_columns = get_filtered_columns(config, [headers_row])

        with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', encoding='utf-8') as temp_output_file:
            temp_output_path = temp_output_file.name

        with tqdm(total=row_count, desc="Tokenizing and writing to temp file", unit="rows") as pbar_tokenizing:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_tasks) as executor:
                futures = []
                for offset in range(0, row_count, rows_per_chunk):
                    #logging.info(f"Processing rows from offset {offset} to {min(offset + rows_per_chunk, row_count)}")
                    input_data = fetch_file_data(data_rows, offset, rows_per_chunk)
                    future = executor.submit(process_chunk, input_data, headers, filtered_columns, api_url, write_skip_columns_as_is, skip_columns, all_columns, int32_columns)
                    futures.append(future)

                    if len(futures) >= max_parallel_tasks or offset + rows_per_chunk >= row_count:
                        for completed_future in concurrent.futures.as_completed(futures):
                            try:
                                result = completed_future.result()
                                with open(temp_output_path, mode='a', newline='', encoding='utf-8') as temp_output_file:
                                    writer = csv.writer(temp_output_file, dialect=file_dialect, escapechar="\\")
                                    writer.writerows(result)
                                pbar_tokenizing.update(len(result))
                            except Exception as e:
                                logging.error(f"Exception during future execution: {str(e)}")
                        futures = []

        with open(temp_output_path, mode='r', encoding='utf-8') as temp_output_file:
            reader = csv.reader(temp_output_file, dialect=file_dialect, escapechar="\\")
            tokenized_data = list(reader)

        with tqdm(total=len(tokenized_data), desc="Writing to destination file", unit="rows") as pbar_writing:
            # Write the headers first, followed by the tokenized data
            write_tokenized_data_to_file(config, tokenized_data, pbar_writing, headers_row, file_dialect)

        os.remove(temp_output_path)
        logging.info("Processing completed successfully")

    else:
        logging.error("Error: 'INPUT_FILE' and/or 'OUTPUT_FILE' sections are missing in the configuration file.")
        exit(1)
