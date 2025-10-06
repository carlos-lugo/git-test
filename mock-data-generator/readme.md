# Project: `fossy_stg` Data Utilities - Setup and Configuration

This guide covers setting up an isolated Python environment and configuring scripts to manage data for the `fossy_stg` DynamoDB table. It includes a script for generating mock data and a utility script for cleaning the table. The project uses a virtual environment for dependencies like Boto3 (AWS SDK) and Faker (mock data).

## Overview

This project provides two main scripts:

  * **`generate_fossy_mock_data.py`**: Creates and inserts a configurable number of mock data records into the `fossy_stg` DynamoDB table.
  * **`delete.py`**: Deletes all items from the table *except* for records matching a predefined list of usernames.

-----

## Prerequisites

  * Python 3 installed.
  * An AWS CLI profile configured with credentials that have read/write access to the `fossy_stg` DynamoDB table.
  * **Table Write Capacity (Important for Cost and Performance)**: Running these scripts involves thousands of write operations, which can cause errors if the table's capacity is too low. You have two options:
      * **On-Demand Mode (Recommended)**: The easiest method is to switch your table to **On-Demand** capacity mode. It automatically handles the write requests and you only pay for what you use.
      * **Provisioned Mode (Manual Control)**: If you use Provisioned mode, you must temporarily increase the capacity.
        1.  **Before** running a script, go to your table's **Capacity** tab in the AWS Console and increase the **Write Capacity Units (WCU)** to a higher value (e.g., **100**).
        2.  **After** the script finishes, it is crucial to return to this screen and **lower the WCU back down to 1** to avoid unnecessary costs.

-----

## Environment Setup

Execute these commands in your project directory:

1.  **Create Virtual Environment:**

    ```bash
    python3 -m venv .venv
    ```

      * Creates an isolated Python environment in the `.venv` directory.

2.  **Activate Virtual Environment:**

    ```bash
    source .venv/bin/activate
    ```

      * Activates the environment. Your terminal prompt will usually change to show `(.venv)`.

3.  **Install Required Libraries:**

    ```bash
    pip3 install boto3 Faker
    ```

      * Installs Boto3 and Faker into the active `.venv` environment.

-----

## Configuring the Python Scripts

Before running the scripts, you may need to adjust the settings within each file.

### 1\. Data Generation Script (`generate_fossy_mock_data.py`)

  * **AWS Profile & Region:** Locate the AWS setup section and modify the `AWS_PROFILE_NAME` and `region_name` variables.

    ```python
    # AWS Setup
    TABLE_NAME = "fossy_stg" 
    AWS_PROFILE_NAME = "your-profile-name"  # 👈 *** Replace with your AWS profile name ***
    session = boto3.Session(profile_name=AWS_PROFILE_NAME)
    dynamodb_client = session.client('dynamodb', region_name='ap-northeast-1') # 👈 *** Ensure region is correct ***
    ```

  * **Number of Records:** Find the main execution block and modify the `number_of_records_to_generate` variable.

    ```python
    # --- Main execution ---
    if __name__ == "__main__":
        number_of_records_to_generate = 1000 # 👈 *** Set how many records you want ***
    ```

  * **Pacing (Speed Control):** If you cannot change the table's capacity settings, you can slow down the script to avoid errors. Add a `time.sleep()` delay inside the batch processing loop. A delay of `0.3` seconds works well for a table with **100 WCU**.

    ```python
    import time # 👈 *** Add this to the top of the script ***

    # ... inside the batch_insert_records function's loop ...
    for i in range(0, len(all_statements), batch_size):
        # ... try/except block ...
        
        time.sleep(0.3) # 👈 *** Add this delay at the end of the loop ***
    ```

### 2\. Deletion Script (`delete.py`)

  * **AWS Profile & Region:** Ensure the `AWS_PROFILE_NAME` and `region_name` are set correctly, just like in the generation script.

    ```python
    # --- Configuration ---
    TABLE_NAME = "fossy_stg"
    USERNAMES_TO_KEEP = ["carlos", "carlos-admin"] # 👈 *** Adjust which users to keep ***
    AWS_PROFILE_NAME = "your-profile-name" # 👈 *** Replace with your AWS profile name ***
    session = boto3.Session(profile_name=AWS_PROFILE_NAME)
    dynamodb_client = session.client('dynamodb', region_name='ap-northeast-1') # 👈 *** Ensure region is correct ***
    ```

  * **Usernames to Keep:** Modify the `USERNAMES_TO_KEEP` list to specify which records should **not** be deleted.

-----

## Running the Scripts

With the virtual environment active and the scripts configured:

### 1\. Generating Data

```bash
python3 generate_fossy_mock_data.py
```

  * This will connect to DynamoDB and insert the configured number of mock records.

### 2\. Deleting Data

> ⚠️ **Warning: Destructive Operation**
> The `delete.py` script will permanently remove data from your table. It is highly recommended to **back up your table** before running this script.

1.  **Review the `USERNAMES_TO_KEEP` list** in `delete.py` one last time.

2.  **Execute the script:**

    ```bash
    python3 delete.py
    ```

3.  **Confirm the action:** The script will ask for confirmation in the terminal. You must type `yes` to proceed.

    ```
    This will delete all items from 'fossy_stg' except where username is in ['carlos', 'carlos-admin'].
    Have you backed up your table? (yes/no): yes
    ```

-----

## Exiting the Virtual Environment

When you're finished, deactivate the environment:

```bash
deactivate
```