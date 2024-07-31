import csv
import time
import requests
import argparse
import concurrent.futures
import tempfile
import os
import logging
from tqdm import tqdm
from skyflow.service_account import generate_bearer_token, is_expired

# Set up argument parser
parser = argparse.ArgumentParser(description="Insert data into Skyflow vault and write tokens to output CSV. This is an open-source community script, and Skyflow bears no responsibility for its usage.")
parser.add_argument('--xid', required=True, help='Required: Skyflow account ID')
parser.add_argument('--vurl', required=True, help='Required: Vault URL (e.g., ebfc9bee4242.vault.skyflowapis.com, do not add https:// in url)')
parser.add_argument('--vid', required=True, help='Required: Vault ID')
parser.add_argument('--t', required=True, help='Required: Table name in the vault')
parser.add_argument('--pc', help='Required: Path to the credentials JSON file (either --pc or --bt must be specified)')
parser.add_argument('--bt', help='Required: Bearer token for API calls (either --pc or --bt must be specified)')
parser.add_argument('--pi', required=True, help='Required: Path to the input CSV file containing PII data')
parser.add_argument('--po', required=True, help='Required: Path to the output CSV file for tokenized data')
parser.add_argument('--r', type=int, default=25, help='Optional: Number of rows to be inserted per API call (default: 25)')
parser.add_argument('--mt', type=int, default=5, choices=range(1, 8), help='Optional: Maximum number of parallel API calls (default: 5, max: 7)')
parser.add_argument('--ff', default='failed_records.csv', help='Optional: Path to the CSV file for failed records (default: failed_records.csv)')
parser.add_argument('--log', default='error.log', help='Optional: Path to the log file (default: error.log)')
args = parser.parse_args()

# Ensure either --pc or --bt is specified, but not both
if bool(args.pc) == bool(args.bt):
    parser.error('Either --pc or --bt must be specified, but not both.')

# Setup logging
logging.basicConfig(filename=args.log, level=logging.ERROR)

# Variables from command line parameters
X_SKYFLOW_ACCOUNT_ID = args.xid
VAULT_URL = args.vurl
VAULT_ID = args.vid
TABLE_NAME = args.t
PATH_TO_CREDENTIALS_JSON = args.pc
BEARER_TOKEN = args.bt
PATH_TO_INPUT_CSV = args.pi
PATH_TO_OUTPUT_CSV = args.po
FAILED_RECORDS_FILE = args.ff
ROWS_PER_CHUNK = args.r
MAX_PARALLEL_TASKS = args.mt
MAX_CALLS_PER_MINUTE = 70

# Function to generate bearer token
bearerToken = ''
tokenType = ''
def token_provider():
    global bearerToken
    global tokenType
    if not BEARER_TOKEN:
        if is_expired(bearerToken):
            bearerToken, tokenType = generate_bearer_token(PATH_TO_CREDENTIALS_JSON)
    else:
        bearerToken = BEARER_TOKEN
        tokenType = "Bearer"
    return bearerToken, tokenType

# Function to make API call with retries
def make_api_call(payload, headers):
    api_url = f"https://{VAULT_URL}/v1/vaults/{VAULT_ID}/{TABLE_NAME}"
    retries = 3
    for attempt in range(retries):
        response = requests.post(api_url, json=payload, headers=headers)
        if response.status_code == 200:
            return response
        else:
            logging.error(f"Attempt {attempt + 1} failed with status code {response.status_code}: {response.text}")
            time.sleep(1)  # Simple backoff strategy
    return response

# Function to process a chunk of rows
def process_chunk(rows, headers, fieldnames, temp_output_path, failed_records):
    payload = {
        "quorum": False,
        "records": [{"fields": row} for row in rows],
        "tokenization": True
    }

    response = make_api_call(payload, headers)
    if response.status_code == 200:
        response_data = response.json()
        if 'records' in response_data and all('tokens' in record for record in response_data['records']):
            pass  # Tokenization successful
        else:
            logging.error(f"Tokenization unsuccessful. Response: {response_data}")

        with open(temp_output_path, mode='a', newline='') as tempfile_output:
            writer = csv.DictWriter(tempfile_output, fieldnames=['skyflow_id'] + fieldnames)
            for original_row, record in zip(rows, response_data.get('records', [])):
                row = {'skyflow_id': record.get('skyflow_id')}
                tokens = record.get('tokens', {})
                for field in fieldnames:
                    row[field] = tokens.get(field, original_row.get(field))
                writer.writerow(row)
    else:
        logging.error(f"Error occurred: {response.text}")
        with open(failed_records, mode='a', newline='') as failed_file:
            writer = csv.DictWriter(failed_file, fieldnames=fieldnames)
            writer.writerows(rows)

# Generate bearer token
accessToken, tokenType = token_provider()
headers = {
    "X-SKYFLOW-ACCOUNT-ID": X_SKYFLOW_ACCOUNT_ID,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {accessToken}"
}

# Create a temporary file to buffer the output
with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='') as temp_output_file:
    temp_output_path = temp_output_file.name

# Create or clear the failed records file
with open(FAILED_RECORDS_FILE, mode='w', newline='') as failed_file:
    pass

# Read CSV and prepare data in chunks
start_time = time.time()
with open(PATH_TO_INPUT_CSV, mode='r') as infile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames

    # Write header to temporary file
    with open(temp_output_path, mode='a', newline='') as temp_output_file:
        writer = csv.DictWriter(temp_output_file, fieldnames=['skyflow_id'] + fieldnames)
        writer.writeheader()

    chunks = []
    while True:
        rows = [next(reader, None) for _ in range(ROWS_PER_CHUNK)]
        rows = [row for row in rows if row]  # Remove None values
        if not rows:
            break  # Exit loop if no more data
        chunks.append((rows, headers, fieldnames, temp_output_path, FAILED_RECORDS_FILE))

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PARALLEL_TASKS) as executor:
        future_to_chunk = {executor.submit(process_chunk, *chunk): chunk for chunk in chunks}

        with tqdm(total=len(chunks), desc="Processing") as pbar:
            for future in concurrent.futures.as_completed(future_to_chunk):
                try:
                    future.result()
                except Exception as exc:
                    logging.error(f"Generated an exception: {exc}")
                pbar.update(1)

# Move data from temporary file to final output file
with open(temp_output_path, mode='r') as temp_output_file, open(PATH_TO_OUTPUT_CSV, mode='w', newline='') as outfile:
    writer = csv.writer(outfile)
    for row in csv.reader(temp_output_file):
        writer.writerow(row)

# Clean up temporary file
os.remove(temp_output_path)

total_time = time.time() - start_time
print(f"API calls completed and data written to the output CSV. Total time taken: {total_time:.2f} seconds.")

