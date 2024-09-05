# Tokenize Data with Skyflow and CSV Files

## Overview

This Python script facilitates the process of tokenizing sensitive data using Skyflow's Data Privacy Vault and storing the tokenized data into a CSV file. The script reads data from a source CSV file, sends it to Skyflow for tokenization, and then writes the tokenized data, along with a `skyflow_id`, to a destination CSV file.

## Features

- **Data Fetching**: Extracts data from a specified CSV file.
- **Tokenization**: Utilizes Skyflow's API to tokenize sensitive fields.
- **Data Writing**: Saves the tokenized data into a destination CSV file.

## Prerequisites

- Python 3.x
- `requests` library (`pip install requests`)
- `tqdm` library (`pip install tqdm`)
- Skyflow account credentials
- A CSV file containing the data to be tokenized

## Installation

1. **Install the required Python packages**:
    ```bash
    pip install -r requirements.txt
    ```

2 **Set up schema in Skyflow vault**:
   - Ensure to create a table in Skyflow vault with matching field names as in source CSV file. The script will fail if the field names in CSV header row and Skyflow table don't match.
   - Create a service account with appropriate permissions to insert the data in the vault table. Make sure you have either a credentials.json or API key for the service account.
   - Also please note that if you have tokenization disabled on certain fields in the vault, the script by default will write the source data as is into destination CSV file for that field.

## Configuration

Before running the script, you need to configure the script using a configuration file. The configuration file is in the `INI` format and includes several sections.

##  
### Configuration File Structure


The configuration file is divided into several sections:

1. **[DEFAULT]**:
    - `skyflow_account_id`: Your Skyflow account ID.
    - `vault_url`: Skyflow Vault URL (e.g., `ebfc9bee4242.vault.skyflowapis.com`).
    - `vault_id`: The ID of your Skyflow vault.
    - `table_name`: The name of the table in the Skyflow vault.
    - `path_for_credentials_json_file`: Path to the Skyflow credentials JSON file.
    - `api_bearer_token`: Optional bearer token for API calls (used if the credentials JSON file is not specified).
    - `rows_per_chunk`: Number of rows to process per API call (default: 25).
    - `max_parallel_tasks`: Maximum number of parallel API calls (default: 5, max: 7).
    - `max_calls_per_minute`: Maximum API calls per minute (default: 70).

2. **[INPUT_CSV]**:
    - `input_csv_file_path=`: Full path of input csv file


3. **[OUTPUT_CSV]**:
    - `output_csv_file_path=`: Full path of output csv file

4. **[COLUMN_SKIP]**:
    - `skip_columns`: Comma-separated list of columns to skip while reading the source data.
    - `write_skip_columns_as_is`: Boolean value to indicate whether the skipped columns should be written as-is to the destination CSV (default: False).

### Configuration File Example (`config_csv.ini`)

```ini
[DEFAULT]
# Skyflow configuration parameters

skyflow_account_id=
vault_url=
vault_id=
table_name=
path_for_credentials_json_file=
api_bearer_token=
rows_per_chunk=25
max_parallel_tasks=5
max_calls_per_minute=70

[INPUT_CSV]
# Configuration for the input CSV File
input_csv_file_path=

[OUTPUT_CSV]
# Configuration for the output CSV File
output_csv_file_path=

[COLUMN_SKIP]
skip_columns=
write_skip_columns_as_is=False
```

## Usage

1. **Generate the blank configuration file template (if you haven't already)**: 
   ```bash
   python3 tokenize_data_csv.py --gen-config-csv
   ```

2. **Run the Script**:
   Execute the script by providing the path to your configuration file:

   ```bash
   python3 tokenize_data_csv.py --config-file config_csv.ini
   ```

3. **Output**:
   The tokenized data will be written to the CSV file specified in the `output_csv_file_path` field of your configuration file.

## Logging

Errors and process information are logged to an `error.log` file in the same directory as the script. This log file will capture any issues that occur during execution, such as API errors or data processing issues.

## Example

```bash
python3 tokenize_data_csv.py --config-file config_csv.ini
```

This command will read data from the input CSV file, tokenize it using Skyflow, and write the results to the output CSV file.

## Notes

- Ensure that your input CSV file is properly formatted and that the columns you intend to tokenize are correctly specified in the configuration file.
- The script assumes that the first row of the CSV file contains the column headers.
- If a `bearer_token` is provided, the script will use it for API calls; otherwise, it will generate one using the credentials JSON file.
