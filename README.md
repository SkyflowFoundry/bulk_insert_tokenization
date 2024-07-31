# Skyflow Vault Bulk Data Insert and Tokenization Script

This script inserts and tokenizes data using Skyflow vault. It reads PII data from an input CSV, makes API calls to Skyflow for tokenization, and writes the tokenized data to an output CSV.

## Features

- Inserts data into Skyflow vault
- Tokenizes data for secure storage
- Handles retries and logging for API calls
- Supports parallel processing for efficient data insertion

## Requirements

- Python 3.7+
- Skyflow Python SDK
- `requests` library
- `argparse` library
- `concurrent.futures` library
- `tqdm` library


## Installation

1. Clone this repository:
    ```sh
    git clone <repository-url>
    ```
2. Navigate to the project directory:
    ```sh
    cd <repository-directory>
    ```
3. Install the required Python packages:
    ```sh
    pip install requests argparse tqdm
    ```

## Usage

python insert_and_tokenize.py --xid <Skyflow account ID> --vurl <Vault URL> --vid <Vault ID> --t <Table name> --pc <Path to credentials JSON> --pi <Path to input CSV> --po <Path to output CSV>

## Example
python insert_and_tokenize.py --xid YOUR_ACCOUNT_ID --vurl YOUR_VAULT_URL --vid YOUR_VAULT_ID --t YOUR_TABLE_NAME --pc path/to/credentials.json --pi path/to/input.csv --po path/to/output.csv

## Arguments

--xid:  Required. Skyflow account ID.
--vurl: Required. Vault URL (e.g., ebfc9bee4242.vault.skyflowapis.com).
--vid:  Required. Vault ID.
--t:    Required. Table name in the vault.
--pc:   Required. Path to the credentials JSON file (either --pc or --bt must be specified).
--bt:   Required. Bearer token for API calls (either --pc or --bt must be specified).
--pi:   Required. Path to the input CSV file containing PII data.
--po:   Required. Path to the output CSV file for tokenized data.
--r:    Optional. Number of rows to be inserted per API call (default: 25).
--mt:   Optional. Maximum number of parallel API calls (default: 5, max: 5).
--ff:   Optional. Path to the CSV file for failed records (default: failed_records.csv).
--log:  Optional. Path to the log file (default: error.log).

## Logging

Errors and failed records are logged in the specified log and failed records files.

## Important Notes:

1. Please this script in the directory with read write permissions
2. This is tested on Python 3.10.12. it may or may not work on lower versions
3. Do not increase the max number of rows to anything more than 25 as this is limit enforced on Skyflow Vault. The script will fail of this number is more than 25.
4. Skyflow Python SDK Should be installed to use this script. Refer to https://github.com/skyflowapi/skyflow-python?tab=readme-ov-file#installation for installing the skyflow SDK.
5. If you are going to use credentials.json file then make sure the file is in same directory or full path of the file is specified

## License

This is an open-source community script, and Skyflow bears no responsibility for its usage.
