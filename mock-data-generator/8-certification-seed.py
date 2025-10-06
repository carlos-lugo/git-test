import boto3
import uuid
import random
from datetime import datetime, timezone, timedelta
import time
from boto3.dynamodb.types import TypeSerializer # Used for handling complex data types

# --- Partition Structures and Relationships ---
# This script generates mock data for the CERTIFICATION partition.
#
# Basic Partitions (Independent):
# - STUDENT: Represents individual student data.
# - EXAM_PLACE: Represents available exam locations.
# - BANK_ACCOUNT: Represents bank accounts for payments.
#
# Dependent Partitions:
# - EXAM: Depends on BANK_ACCOUNT (for exam fees).
# - EXAM_HOLD: Depends on EXAM and EXAM_PLACE.
# - APPLICATION: Depends on STUDENT, EXAM_HOLD.
# - PAYMENT: Depends on APPLICATION.
# - CERTIFICATION: Depends on APPLICATION.
# ---------------------------------------------

# --- Configuration ---
# Set your DynamoDB table name and AWS profile
TABLE_NAME = "fossy_stg"
AWS_PROFILE_NAME = "asdf"
# ---------------------

# AWS Setup
print(f"Using AWS Profile: {AWS_PROFILE_NAME}")
session = boto3.Session(profile_name=AWS_PROFILE_NAME)
dynamodb_client = session.client('dynamodb', region_name='ap-northeast-1')

def get_existing_items(partition_key):
    """Queries DynamoDB to get all items with a given partition key."""
    try:
        response = dynamodb_client.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="partitionKey = :pk",
            ExpressionAttributeValues={":pk": {"S": partition_key}}
        )
        items = response.get('Items', [])
        if items:
            print(f"✅ Successfully fetched {len(items)} items for {partition_key}")
        else:
            print(f"⚠️ Warning: Found 0 items with partition key '{partition_key}'")
        return items
    except Exception as e:
        print(f"❌ Error fetching items for {partition_key}: {e}")
        return []

def create_hardcoded_certification_data(applications):
    """
    Creates a list of hardcoded mock certification records, linking them to existing applications.
    """
    now_utc = datetime.now(timezone.utc)
    created_by_user = "system_seed_script"

    certifications = []

    for application in applications:
        # Only create certifications for completed payments
        if application['paymentStatus']['S'] == 'completed':
            certification = {
                "partitionKey": "CERTIFICATION",
                "sortKey": str(uuid.uuid4()),
                "applicationId": application['sortKey']['S'],
                "studentId": application['studentId']['S'],
                "examId": application['examId']['S'],
                "examName": application['examName']['S'],
                "issueDate": now_utc.strftime('%Y-%m-%d'),
                "expirationDate": (now_utc + timedelta(days=365*2)).strftime('%Y-%m-%d'), # 2 years expiration
                "certificationNumber": f"CERT-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}",
                "status": "active",
                "memo": f"Mock certification for application {application['sortKey']['S']}",
                "createdBy": created_by_user,
                "createdOn": now_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
                "updatedBy": created_by_user,
                "updatedOn": now_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
            }
            certifications.append(certification)

    # Add standard fields to each exam record
    serializer = TypeSerializer()
    final_records = []
    for record in certifications:
        # Convert all values to string first, as per schema, except for specific types
        for key, value in record.items():
            if not isinstance(value, (list, int, bool)):
                record[key] = str(value)

        # The serializer converts the Python dictionary to DynamoDB's format
        # This correctly handles lists, maps, strings, and numbers
        dynamodb_item = {k: serializer.serialize(v) for k, v in record.items() if v != ''}
        final_records.append(dynamodb_item)

    return final_records

def batch_insert_records(records_to_insert, batch_size=25):
    """Inserts records into DynamoDB using BatchWriteItem, which is ideal for complex data."""
    successful_inserts = 0
    total_records = len(records_to_insert)

    # Format for BatchWriteItem
    put_requests = [{"PutRequest": {"Item": item}} for item in records_to_insert]

    for i in range(0, len(put_requests), batch_size):
        batch = put_requests[i:i + batch_size]
        request_items = {TABLE_NAME: batch}
        try:
            response = dynamodb_client.batch_write_item(RequestItems=request_items)
            
            # Basic success count - BatchWriteItem is often all-or-nothing unless there are UnprocessedItems
            success_in_batch = len(batch)
            if 'UnprocessedItems' in response and response['UnprocessedItems']:
                unprocessed_count = len(response['UnprocessedItems'].get(TABLE_NAME, []))
                success_in_batch -= unprocessed_count
                print(f"  ⚠️ {unprocessed_count} items were not processed and should be retried.")

            successful_inserts += success_in_batch
            print(f"Processed batch of {len(batch)}. Succeeded: {success_in_batch}. (Total Inserted: {successful_inserts}/{total_records})")

        except Exception as e:
            print(f"❌ An exception occurred processing batch starting at index {i}: {e}")
        
        time.sleep(1)

# --- Main execution ---
if __name__ == "__main__":
    print(f"🚀 Starting script to insert mock certifications into table '{TABLE_NAME}'...")
    
    # 1. Get existing application items
    applications = get_existing_items("APPLICATION")

    # 2. Proceed only if applications were found
    if applications:
        # 3. Generate the list of certification records
        certification_data_list = create_hardcoded_certification_data(applications)
        number_of_records = len(certification_data_list)
        print(f"Generated {number_of_records} certification records. Starting batch insert...")
        
        # 4. Insert the records into DynamoDB
        batch_insert_records(certification_data_list)
        
        print("✅ All certification data insertion attempts complete.")
    else:
        print("❌ Script stopped. Cannot create certifications without existing applications to link.")
