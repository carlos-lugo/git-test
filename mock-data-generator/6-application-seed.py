import boto3
import uuid
import random
from datetime import datetime, timezone, timedelta
import time
from boto3.dynamodb.types import TypeSerializer # Used for handling complex data types

# --- Partition Structures and Relationships ---
# This script generates mock data for the APPLICATION partition.
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

def create_hardcoded_application_data(students, exam_holds):
    """
    Creates a list of hardcoded mock application records, linking them to existing students and exam holds.
    """
    now_utc = datetime.now(timezone.utc)
    created_by_user = "system_seed_script"

    applications = []

    for student in students:
        # Each student will apply for one random exam hold
        exam_hold = random.choice(exam_holds)

        application = {
            "partitionKey": "APPLICATION",
            "sortKey": str(uuid.uuid4()),
            "studentId": student['sortKey']['S'],
            "examHoldId": exam_hold['sortKey']['S'],
            "examId": exam_hold['examId']['S'],
            "examName": exam_hold['examName']['S'],
            "examDate": exam_hold['examHoldDate']['S'],
            "examPlace": exam_hold['examHoldPlace'], # This is a list of maps
            "applicationDate": now_utc.strftime('%Y-%m-%d'),
            "paymentMethod": random.choice(["credit_card", "bank_transfer"]),
            "paymentStatus": random.choice(["pending", "completed", "failed"]),
            "examFee": exam_hold['examFee']['S'],
            "lessonFee": exam_hold.get('lessonFee', {'S': '0'})['S'],
            "certificationFee": exam_hold.get('certificationFee', {'S': '0'})['S'],
            "totalFee": str(int(exam_hold['examFee']['S']) + int(exam_hold.get('lessonFee', {'S': '0'})['S']) + int(exam_hold.get('certificationFee', {'S': '0'})['S'])),
            "memo": f"Mock application for {student['firstName']['S']} {student['lastName']['S']}",
            "createdBy": created_by_user,
            "createdOn": now_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
            "updatedBy": created_by_user,
            "updatedOn": now_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
        }
        applications.append(application)

    # Add standard fields to each exam record
    serializer = TypeSerializer()
    final_records = []
    for record in applications:
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
    print(f"🚀 Starting script to insert mock applications into table '{TABLE_NAME}'...")
    
    # 1. Get existing student and exam hold items
    students = get_existing_items("STUDENT")
    exam_holds = get_existing_items("EXAM_HOLD")

    # 2. Proceed only if students and exam holds were found
    if students and exam_holds:
        # 3. Generate the list of application records
        application_data_list = create_hardcoded_application_data(students, exam_holds)
        number_of_records = len(application_data_list)
        print(f"Generated {number_of_records} application records. Starting batch insert...")
        
        # 4. Insert the records into DynamoDB
        batch_insert_records(application_data_list)
        
        print("✅ All application data insertion attempts complete.")
    else:
        print("❌ Script stopped. Cannot create applications without existing students and exam holds to link.")
