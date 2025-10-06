import boto3
import uuid
from datetime import datetime, timezone
import time

# --- Configuration ---
# Set your DynamoDB table name and AWS profile
TABLE_NAME = "fossy_stg"
AWS_PROFILE_NAME = "asdf"
# ---------------------

# AWS Setup
print(f"Using AWS Profile: {AWS_PROFILE_NAME}")
session = boto3.Session(profile_name=AWS_PROFILE_NAME)
dynamodb_client = session.client('dynamodb', region_name='ap-northeast-1')

def create_hardcoded_bank_account_data():
    """
    Creates a list of 5 hardcoded mock bank account records.
    All data is predefined to ensure consistency.
    """
    # Use a single timestamp for all records for consistency
    now_utc = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    created_by_user = "system_seed_script"

    accounts = [
        {
            "bankName": "三菱UFJ銀行",
            "branchName": "渋谷支店",
            "depositType": "普通",
            "accountNumber": "1234567",
            "accountHolder": "一般財団法人 職業技能振興会"
        },
        {
            "bankName": "三井住友銀行",
            "branchName": "新宿西口支店",
            "depositType": "普通",
            "accountNumber": "7654321",
            "accountHolder": "ザイ）ショクギョウギノウシンコウカイ"
        },
        {
            "bankName": "みずほ銀行",
            "branchName": "銀座中央支店",
            "depositType": "当座",
            "accountNumber": "0112233",
            "accountHolder": "一般財団法人 職業技能振興会"
        },
        {
            "bankName": "ゆうちょ銀行",
            "branchName": "〇一八支店", # A common format for JP Post Bank branches
            "depositType": "普通",
            "accountNumber": "10180-12345671",
            "accountHolder": "職業技能振興会"
        },
        {
            "bankName": "楽天銀行",
            "branchName": "第一営業支店",
            "depositType": "普通",
            "accountNumber": "7009988",
            "accountHolder": "ザイ）ショクギョウギノウシンコウカイ"
        }
    ]

    # Add standard fields to each account record
    for record in accounts:
        record["partitionKey"] = "BANK_ACCOUNT"
        record["sortKey"] = str(uuid.uuid4())
        record["createdBy"] = created_by_user
        record["createdOn"] = now_utc
        record["updatedBy"] = created_by_user
        record["updatedOn"] = now_utc
        # Add memo field from the schema as an empty string
        record.setdefault("memo", "")

    return accounts

def batch_insert_records(records_to_insert, total_records_to_generate, batch_size=25):
    """Inserts records into DynamoDB using BatchExecuteStatement with PartiQL."""
    all_statements = []
    for record_item in records_to_insert:
        value_map_parts = []
        for key, value in record_item.items():
            # Escape single quotes for PartiQL statement
            escaped_value = str(value).replace("'", "''")
            value_map_parts.append(f"'{key}':'{escaped_value}'")

        value_map_str = "{" + ", ".join(value_map_parts) + "}"
        statement_str = f"INSERT INTO \"{TABLE_NAME}\" VALUE {value_map_str}"
        all_statements.append({'Statement': statement_str})

    successful_inserts = 0

    # Process statements in batches
    for i in range(0, len(all_statements), batch_size):
        batch = all_statements[i:i + batch_size]
        try:
            response = dynamodb_client.batch_execute_statement(Statements=batch)

            errors_in_batch = 0
            if 'Responses' in response:
                for idx, res in enumerate(response['Responses']):
                    if 'Error' in res:
                        errors_in_batch += 1
                        print(f"  Error in statement {i+idx}: {res['Error']['Code']} - {res['Error']['Message']}")

            success_in_batch = len(batch) - errors_in_batch
            successful_inserts += success_in_batch

            print(f"Processed batch of {len(batch)}. Succeeded: {success_in_batch}. (Total Inserted: {successful_inserts}/{total_records_to_generate})")

        except Exception as e:
            print(f"An exception occurred processing batch starting at index {i}: {e}")

        # Add a small delay to respect provisioned throughput
        time.sleep(1)

# --- Main execution ---
if __name__ == "__main__":
    print(f"Preparing to insert hardcoded bank accounts into table '{TABLE_NAME}'...")

    # Generate the list of 5 bank account records
    bank_account_list = create_hardcoded_bank_account_data()
    number_of_records_to_generate = len(bank_account_list)

    print(f"Generated {number_of_records_to_generate} bank account records. Starting batch insert...")

    # Insert the records into DynamoDB
    batch_insert_records(bank_account_list, number_of_records_to_generate)

    print("All bank account data insertion attempts complete.")

