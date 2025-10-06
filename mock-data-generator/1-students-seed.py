import boto3
from faker import Faker
import uuid
import random
from datetime import datetime, timedelta
import time

# Initialize Faker (use 'ja_JP' for Japanese data, 'en_US' for generic English)
fake_ja = Faker('ja_JP')
fake_en = Faker()

# AWS Setup
TABLE_NAME = "fossy_stg" # Your confirmed table name
AWS_PROFILE_NAME = "asdf"  # Add your profile name here
print(f"Using AWS Profile: {AWS_PROFILE_NAME}") # Optional: for confirmation when running
session = boto3.Session(profile_name=AWS_PROFILE_NAME)
# Add region_name to the client initialization for clarity and correctness
dynamodb_client = session.client('dynamodb', region_name='ap-northeast-1')

# User's current time context (JST), converted to UTC as base for timestamps
current_jst_from_user = datetime(2025, 9, 1, 20, 50, 8) # JST
base_utc_time = current_jst_from_user - timedelta(hours=9) # Convert to UTC

def generate_mock_item_for_fossy_stg():
    """Generates a single mock item dictionary for the fossy_stg table."""
    record = {}
    record["partitionKey"] = "STUDENT"
    record["sortKey"] = str(uuid.uuid4())

    registration_method_original = random.choice(["web", "manual_entry"])
    record["online"] = "true" if registration_method_original == "web" else "false"

    japanese_last_name = fake_ja.last_name()
    japanese_first_name = fake_ja.first_name()
    record["lastName"] = japanese_last_name
    record["firstName"] = japanese_first_name
    record["lastNameKana"] = fake_ja.last_kana_name()
    record["firstNameKana"] = fake_ja.first_kana_name()

    record["birthday"] = fake_ja.date_of_birth(minimum_age=18, maximum_age=65).strftime('%Y-%m-%d')
    record["gender"] = random.choice(["male", "female", "other"])

    record["phoneNumber1"] = f"0{random.choice([7,8,9])}0-{random.randint(1000,9999)}-{random.randint(1000,9999)}"
    record["phoneNumber2"] = f"0{random.randint(3,9)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}" if random.choice([True, False]) else ""
    record["faxNumber"] = ""
    record["email"] = fake_en.email()

    occupation = random.choice(["Engineer", "Teacher", "Doctor", "Office Worker", "Student", ""])
    record["occupation"] = occupation
    record["organization"] = fake_ja.company() if occupation not in ["Student", ""] and random.choice([True,False]) else (fake_ja.word() + "大学" if occupation == "Student" else "")

    # Home Address
    record["postalCode"] = fake_ja.zipcode()
    record["prefecture"] = fake_ja.prefecture()
    city_part = fake_ja.city()
    town_part = fake_ja.town()
    record["city"] = f"{city_part}{town_part if town_part else ''}".strip()
    record["addressLine"] = f"{fake_ja.chome()}-{fake_ja.ban()}-{fake_ja.gou()}"
    record["mansionBuilding"] = fake_ja.building_name() if random.choice([True, False]) else ""

    doc_dest = random.choice(["home", "work"])
    record["sendingAddress"] = doc_dest

    if doc_dest == "work" or (record["organization"] != "" and random.choice([True, False])):
        record["organizationPostalCode"] = fake_ja.zipcode()
        record["organizationPrefecture"] = fake_ja.prefecture()
        record["organizationCity"] = f"{fake_ja.city()}{fake_ja.town() if fake_ja.town() else ''}".strip()
        record["organizationAddressLine"] = f"{fake_ja.chome()}-{fake_ja.ban()}-{fake_ja.gou()}"
        record["organizationMansionBuilding"] = fake_ja.building_name() if random.choice([True, False]) else ""
        record["organizationPN"] = f"0{random.randint(3,9)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}"
    else:
        record["organizationPostalCode"] = ""
        record["organizationPrefecture"] = ""
        record["organizationCity"] = ""
        record["organizationAddressLine"] = ""
        record["organizationMansionBuilding"] = ""
        record["organizationPN"] = ""
    
    record["noticeName"] = fake_ja.name() if random.choice([True,False,False]) else ""
    record["noticeStudent"] = record["firstName"] + " " + record["lastName"] if record["noticeName"] != "" else ""
    record["resignation"] = "" # Typically empty unless specific scenario

    # Timestamps and user IDs
    generated_username = fake_en.user_name()
    record["username"] = generated_username
    
    created_on = base_utc_time - timedelta(days=random.randint(1, 365), hours=random.randint(0,23), minutes=random.randint(0,59))
    update_duration_days = (base_utc_time - created_on).days
    updated_on_offset_days = random.randint(0, update_duration_days if update_duration_days > 0 else 0)
    updated_on = created_on + timedelta(days=updated_on_offset_days, hours=random.randint(0,23), minutes=random.randint(0,59))
    
    if updated_on > base_utc_time: updated_on = base_utc_time
    if updated_on < created_on: updated_on = created_on

    record["createdOn"] = created_on.isoformat(timespec='milliseconds') + "Z"
    record["createdBy"] = "system_batch"
    record["updatedOn"] = updated_on.isoformat(timespec='milliseconds') + "Z"
    record["updatedBy"] = random.choice(["system_update", "admin_portal", generated_username])
    
    # Ensure all known headers get a value, even if empty, to match schema
    all_known_headers = [
        "partitionKey", "sortKey", "addressLine", "birthday", "city", "createdBy", "createdOn", "email",
        "faxNumber", "firstName", "firstNameKana", "gender", "lastName", "lastNameKana", "mansionBuilding",
        "noticeName", "noticeStudent", "occupation", "online", "organization", "organizationAddressLine",
        "organizationCity", "organizationMansionBuilding", "organizationPN", "organizationPostalCode",
        "organizationPrefecture", "phoneNumber1", "phoneNumber2", "postalCode", "prefecture", "resignation",
        "sendingAddress", "updatedBy", "updatedOn", "username"
    ]
    for header in all_known_headers:
        if header not in record:
            record[header] = "" # Add any missing keys as empty strings
            
    return record

# <--- MODIFIED: Function now accepts a 'delay_seconds' parameter ---
def batch_insert_records(records_to_insert, total_records_to_generate, delay_seconds=3, batch_size=25):
    """Inserts records into DynamoDB using BatchExecuteStatement with PartiQL."""
    all_statements = []
    for record_item in records_to_insert:
        value_map_parts = []
        for key, value in record_item.items():
            escaped_value = str(value).replace("'", "''") # Escape single quotes
            value_map_parts.append(f"'{key}':'{escaped_value}'")
        
        value_map_str = "{" + ", ".join(value_map_parts) + "}"
        statement_str = f"INSERT INTO \"{TABLE_NAME}\" VALUE {value_map_str}"
        all_statements.append({'Statement': statement_str})

    successful_inserts = 0

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
            print(f"Error processing batch starting at index {i}: {e}")
        
        # <--- MODIFIED: Uses the 'delay_seconds' parameter for the delay ---
        print(f"Waiting for {delay_seconds} second(s)...")
        time.sleep(delay_seconds)

# --- Main execution ---
if __name__ == "__main__":
    # <--- NEW: Ask user for the number of records to generate ---
    while True:
        try:
            num_input = input("How many student records do you want to generate? ")
            number_of_records_to_generate = int(num_input)
            if number_of_records_to_generate > 0:
                break
            else:
                print("Please enter a positive number.")
        except ValueError:
            print("Invalid input. Please enter a whole number.")

    # <--- NEW: Ask user for the delay, with a default value ---
    try:
        delay_input = input("Enter delay in seconds between batches (default is 3): ")
        batch_delay = int(delay_input)
        if batch_delay < 0:
            print("Delay cannot be negative. Using default of 3 seconds.")
            batch_delay = 3
    except (ValueError, TypeError):
        print("Invalid or no input. Using default delay of 3 seconds.")
        batch_delay = 3
            
    print(f"\nGenerating {number_of_records_to_generate} mock records for table '{TABLE_NAME}'...")
    
    mock_data_list = []
    for _ in range(number_of_records_to_generate):
        mock_data_list.append(generate_mock_item_for_fossy_stg())
    
    print(f"Finished generating data. Starting batch insert with a {batch_delay}-second delay...")
    
    # <--- MODIFIED: Pass the user-defined delay to the function ---
    batch_insert_records(mock_data_list, number_of_records_to_generate, delay_seconds=batch_delay)
    
    print("\nAll mock data insertion attempts complete.")