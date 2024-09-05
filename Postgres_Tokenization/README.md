# Tokenize Data with Skyflow and PostgreSQL

## Overview

This Python script facilitates the process of tokenizing sensitive data using Skyflow's Data Privacy Vault and storing the tokenized data into a PostgreSQL database. The script fetches data from a source PostgreSQL database, sends it to Skyflow for tokenization, and then writes the tokenized data, along with a `skyflow_id`, to a destination PostgreSQL database.

## Features

- **Data Fetching**: Extracts data from a specified table in a PostgreSQL database.
- **Data Tokenization**: Utilizes Skyflow’s API to tokenize sensitive fields.
- **Parallel Processing**: Implements multi-threading to enhance performance by processing data in chunks.
- **Data Insertion**: Inserts tokenized data into a target PostgreSQL table.
- **Error Logging**: Logs errors and failed rows to help with troubleshooting.
- **Customizable Configuration**: Allows customization of various parameters through a configuration file.

## Prerequisites

- Python 3.8 or higher
- PostgreSQL database
- Skyflow API account
- Required Python packages (listed in `requirements.txt` or can be installed via pip)

## Installation

1. **Clone the repository**:
    ```bash
    git clone https://github.com/your-repository/tokenize_data_postgres.git
    cd tokenize_data_postgres
    ```

2. **Install the required Python packages**:
    ```bash
    pip install -r requirements.txt
    ```

3. **Set up your PostgreSQL databases**:
   - Ensure that both the source (input) and target (output) PostgreSQL databases are configured and accessible.
   - Create the necessary tables in both the source and target databases if they don’t already exist.

## Configuration

Before running the script, you need to configure it using a configuration file (e.g., `config_pgsql.ini`).

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

2. **[INPUT_PGSQL]**:
    - `input_pg_host`: Hostname or IP address of the input PostgreSQL database.
    - `input_pg_dbname`: Name of the input PostgreSQL database.
    - `input_pg_user`: Username for accessing the input PostgreSQL database.
    - `input_pg_password`: Password for accessing the input PostgreSQL database.
    - `input_pg_table`: Name of the table in the input PostgreSQL database.
    - `input_pg_port`: Port number for the input PostgreSQL database (default: 5432).

3. **[OUTPUT_PGSQL]**:
    - `output_pg_host`: Hostname or IP address of the output PostgreSQL database.
    - `output_pg_dbname`: Name of the output PostgreSQL database.
    - `output_pg_user`: Username for accessing the output PostgreSQL database.
    - `output_pg_password`: Password for accessing the output PostgreSQL database.
    - `output_pg_table`: Name of the table in the output PostgreSQL database.
    - `output_pg_port`: Port number for the output PostgreSQL database (default: 5432).

4. **[COLUMN_SKIP]**:
    - `skip_columns`: Comma-separated list of columns to skip while reading the source data.
    - `write_skip_columns_as_is`: Boolean value to indicate whether the skipped columns should be written as-is to the destination table (default: False).

##
### Example Configuration File

```ini
[DEFAULT]
skyflow_account_id = your_skyflow_account_id
vault_url = ebfc9bee4242.vault.skyflowapis.com
vault_id = your_vault_id
table_name = your_table_name
path_for_credentials_json_file = /path/to/your/credentials.json
rows_per_chunk = 25
max_parallel_tasks = 5
max_calls_per_minute = 70

[INPUT_PGSQL]
input_pg_host = localhost
input_pg_dbname = your_input_db
input_pg_user = your_db_user
input_pg_password = your_db_password
input_pg_table = your_input_table
input_pg_port = 5432

[OUTPUT_PGSQL]
output_pg_host = localhost
output_pg_dbname = your_output_db
output_pg_user = your_db_user
output_pg_password = your_db_password
output_pg_table = your_output_table
output_pg_port = 5432

[COLUMN_SKIP]
skip_columns = city, pincode
write_skip_columns_as_is = False
```

## Usage

1. Generate the configuration file template (if you haven't already):

```bash
python3 tokenize_data_postgres_v2.py --gen-config-pgsql
```

This generates a config_pgsql.ini file template that you can customize.

2. Run the script:

```bash  
python3 tokenize_data_postgres_v2.py --config-file config_pgsql.ini
```
This will process the data according to your configuration, tokenize the specified fields using Skyflow, and write the results to the output PostgreSQL database.


## Logging and Error Handling


* Log File: Errors and debug information are logged to a file specified in the configuration (default: error.log).
* Failed Records: Records that fail to be processed or inserted are logged to a CSV file (default: failed_records.csv).


## Customization


You can customize the script to:

* Adjust the chunk size and parallel processing settings to suit your environment.
* Modify the column skipping behavior to retain or discard certain columns during processing.
* Extend the logging and error handling as needed.

