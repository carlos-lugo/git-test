import boto3
import uuid
import random
from datetime import datetime, timezone
import time
from boto3.dynamodb.types import TypeSerializer # Used for handling complex data types

# --- Partition Structures and Relationships ---
# This script generates mock data for the EXAM partition.
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

def get_existing_ids(partition_key):
    """Queries DynamoDB to get the sortKeys of all items with a given partition key."""
    try:
        response = dynamodb_client.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="partitionKey = :pk",
            ExpressionAttributeValues={":pk": {"S": partition_key}}
        )
        ids = [item['sortKey']['S'] for item in response.get('Items', [])]
        if ids:
            print(f"✅ Successfully fetched {len(ids)} IDs for {partition_key}")
        else:
            print(f"⚠️ Warning: Found 0 items with partition key '{partition_key}'")
        return ids
    except Exception as e:
        print(f"❌ Error fetching IDs for {partition_key}: {e}")
        return []

def create_hardcoded_exam_data(bank_account_ids):
    """
    Creates a list of 4 hardcoded mock exam records, linking them to existing bank accounts.
    """
    now_utc = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    created_by_user = "system_seed_script"

    exams = [
        {
            "examName": "給与実務能力検定試験２級",
            "timeRequired": "120",
            "score": [
                {"display_name": "知識問題", "perfectScore": "70", "passingScore": "50"},
                {"display_name": "計算問題", "perfectScore": "30", "passingScore": "20"}
            ],
            "scoreComment": "知識問題・計算問題それぞれの得点が配点の40%に満たない場合は、総合得点が合格基準に達していても不合格となります。",
            "examItems": ["筆記用具", "電卓", "本人確認書類"],
            "faceImgRequired": "true",
            "lesson": "true",
            "certificationType": "everyone",
            "certificationTemporaryDeadline": "true",
            "certificationShipped": 14, # DynamoDB client will handle number type
            "certificationPrefix": "KYU",
            "licenseExpirationDate": "2",
            "renewText": "request_only",
            "renewTextInclusion": "false",
            "renewLesson": "request_only",
            "renewLessonInclusion": "false",
            "revisionLawInformation": "true",
            "examFee": "10000",
            "studentFee": "8000",
            "groupFee": "9000",
            "lessonFee": "33000",
            "certificationFee": "2000",
            "renewalFee": "5000",
            "renewTextFee": "3000",
            "renewLessonFee": "15000",
            "specialFee": "",
            "bankAccountId": random.choice(bank_account_ids),
            "examUrl": "https://www.jitsumu-up.jp/com_contents/kyuyo/",
            "memo": "2級は、社会保険や税に関するより詳細な知識が問われます。"
        },
        {
            "examName": "シニアライフ・相続アドバイザー",
            "timeRequired": "60",
            "score": [
                {"display_name": "総合得点", "perfectScore": "100", "passingScore": "70"}
            ],
            "scoreComment": "",
            "examItems": ["筆記用具", "本人確認書類"],
            "faceImgRequired": "true",
            "lesson": "false",
            "certificationType": "everyone",
            "certificationTemporaryDeadline": "false",
            "certificationShipped": 21,
            "certificationPrefix": "SLS",
            "licenseExpirationDate": "1",
            "renewText": "none",
            "renewTextInclusion": "none",
            "renewLesson": "everyone",
            "renewLessonInclusion": "true",
            "revisionLawInformation": "true",
            "examFee": "8800",
            "studentFee": "",
            "groupFee": "7700",
            "lessonFee": "",
            "certificationFee": "1500",
            "renewalFee": "11000",
            "renewTextFee": "",
            "renewLessonFee": "", # Included in renewalFee
            "specialFee": "",
            "bankAccountId": random.choice(bank_account_ids),
            "examUrl": "https://www.jitsumu-up.jp/com_contents/seniorlife/",
            "memo": ""
        },
        {
            "examName": "クレーム対応検定",
            "timeRequired": "50",
            "score": [
                {"display_name": "総合得点", "perfectScore": "100", "passingScore": "80"}
            ],
            "scoreComment": "",
            "examItems": [],
            "faceImgRequired": "false",
            "lesson": "true",
            "certificationType": "request_only",
            "certificationTemporaryDeadline": "false",
            "certificationShipped": 10,
            "certificationPrefix": "CLM",
            "licenseExpirationDate": "", # No expiration
            "renewText": "none",
            "renewTextInclusion": "none",
            "renewLesson": "none",
            "renewLessonInclusion": "none",
            "revisionLawInformation": "false",
            "examFee": "5500",
            "studentFee": "",
            "groupFee": "",
            "lessonFee": "20000",
            "certificationFee": "3000",
            "renewalFee": "",
            "renewTextFee": "",
            "renewLessonFee": "",
            "specialFee": "",
            "bankAccountId": random.choice(bank_account_ids),
            "examUrl": "https://www.jitsumu-up.jp/com_contents/claim/",
            "memo": "オンラインでのみ実施。"
        },
        {
            "examName": "マイナンバー実務検定３級",
            "timeRequired": "60",
            "score": [
                {"display_name": "総合得点", "perfectScore": "100", "passingScore": "70"}
            ],
            "scoreComment": "",
            "examItems": ["筆記用具", "本人確認書類"],
            "faceImgRequired": "true",
            "lesson": "false",
            "certificationType": "everyone",
            "certificationTemporaryDeadline": "true",
            "certificationShipped": 14,
            "certificationPrefix": "MNP",
            "licenseExpirationDate": "2",
            "renewText": "request_only",
            "renewTextInclusion": "false",
            "renewLesson": "none",
            "renewLessonInclusion": "none",
            "revisionLawInformation": "true",
            "examFee": "7700",
            "studentFee": "5500",
            "groupFee": "6600",
            "lessonFee": "",
            "certificationFee": "1500",
            "renewalFee": "3000",
            "renewTextFee": "2500",
            "renewLessonFee": "",
            "specialFee": "",
            "bankAccountId": random.choice(bank_account_ids),
            "examUrl": "https://www.jitsumu-up.jp/com_contents/mynumber/",
            "memo": "３級はマイナンバー制度の基本的な理解度を測ります。"
        }
    ]

    # Add standard fields to each exam record
    serializer = TypeSerializer()
    final_records = []
    for record in exams:
        # Convert all values to string first, as per schema, except for specific types
        for key, value in record.items():
            if not isinstance(value, (list, int)):
                record[key] = str(value)

        # Add standard fields
        record["partitionKey"] = "EXAM"
        record["sortKey"] = str(uuid.uuid4())
        record["createdBy"] = created_by_user
        record["createdOn"] = now_utc
        record["updatedBy"] = created_by_user
        record["updatedOn"] = now_utc

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
    print(f"🚀 Starting script to insert mock exams into table '{TABLE_NAME}'...")
    
    # 1. Get existing bank account IDs to link to exams
    bank_ids = get_existing_ids("BANK_ACCOUNT")

    # 2. Proceed only if bank accounts were found
    if bank_ids:
        # 3. Generate the list of exam records
        exam_data_list = create_hardcoded_exam_data(bank_ids)
        number_of_records = len(exam_data_list)
        print(f"Generated {number_of_records} exam records. Starting batch insert...")
        
        # 4. Insert the records into DynamoDB
        batch_insert_records(exam_data_list)
        
        print("✅ All exam data insertion attempts complete.")
    else:
        print("❌ Script stopped. Cannot create exams without existing bank accounts to link.")