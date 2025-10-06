import boto3
import uuid
import random
from datetime import datetime, timezone, timedelta
import time
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

# --- Partition Structures and Relationships ---
# This script generates mock data for the EXAM_HOLD partition.
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
TABLE_NAME = "fossy_stg"
AWS_PROFILE_NAME = "asdf"
NUMBER_OF_SCHEDULES_TO_CREATE = 5 # How many schedules to generate
# ---------------------

# AWS Setup
print(f"Using AWS Profile: {AWS_PROFILE_NAME}")
session = boto3.Session(profile_name=AWS_PROFILE_NAME)
dynamodb_client = session.client('dynamodb', region_name='ap-northeast-1')

def get_full_items_by_pk(partition_key):
    """
    Queries DynamoDB to get all full items for a given partition key.
    Handles pagination and deserializes items into Python dictionaries.
    """
    print(f"🔍 Fetching all items for partitionKey '{partition_key}'...")
    items = []
    deserializer = TypeDeserializer()
    last_evaluated_key = None

    while True:
        query_args = {
            'TableName': TABLE_NAME,
            'KeyConditionExpression': 'partitionKey = :pk',
            'ExpressionAttributeValues': {':pk': {'S': partition_key}}
        }
        if last_evaluated_key:
            query_args['ExclusiveStartKey'] = last_evaluated_key
        
        try:
            response = dynamodb_client.query(**query_args)
            deserialized_items = [deserializer.deserialize({'M': item}) for item in response.get('Items', [])]
            items.extend(deserialized_items)
            
            last_evaluated_key = response.get('LastEvaluatedKey', None)
            if not last_evaluated_key:
                break
        except Exception as e:
            print(f"❌ Error fetching items for '{partition_key}': {e}")
            return []

    if items:
        print(f"✅ Successfully fetched {len(items)} items for '{partition_key}'")
    else:
        print(f"⚠️ Warning: Found 0 items with partition key '{partition_key}'")
    return items


def create_mock_schedule_data(all_exams, all_venues):
    """
    Creates a list of mock exam schedules. It now inherits a comprehensive set of
    attributes from the parent exam to create a complete record.
    """
    now_utc = datetime.now(timezone.utc)
    today_date = now_utc.date()
    created_by_user = "system_seed_script"
    
    schedules = []
    
    for i in range(NUMBER_OF_SCHEDULES_TO_CREATE):
        chosen_exam = random.choice(all_exams)
        num_venues = random.randint(1, min(2, len(all_venues)))
        chosen_venues = random.sample(all_venues, num_venues)
        
        exam_hold_places = [
            {"placeId": v['sortKey'], "capacity": str(random.randint(5, 20) * 10)} for v in chosen_venues
        ]
        prefectures = sorted(list(set([v['prefecture'] for v in chosen_venues])))

        exam_hold_date = today_date + timedelta(days=random.randint(45, 120) + (i * 10))
        application_to_date = exam_hold_date - timedelta(days=10)
        application_from_date = application_to_date - timedelta(days=30)
        result_date = exam_hold_date + timedelta(days=21)
        
        start_hour = random.randint(10, 13)
        start_time_obj = datetime.strptime(f"{start_hour}:00", "%H:%M")

        schedule = {
            # --- Key Identifiers ---
            "examId": chosen_exam['sortKey'],
            "examName": chosen_exam['examName'],
            "examHoldNo": i + 1,
            "bankAccountId": chosen_exam['bankAccountId'],
            
            # --- Comprehensive Details Inherited from Parent Exam ---
            "timeRequired": chosen_exam.get('timeRequired', '60'),
            "score": chosen_exam.get('score', []),
            "scoreComment": chosen_exam.get('scoreComment', ''),
            "examItems": chosen_exam.get('examItems', []),
            "faceImgRequired": chosen_exam.get('faceImgRequired', 'false'),
            "lesson": chosen_exam.get('lesson', 'false'),
            "certificationType": chosen_exam.get('certificationType', 'none'),
            "certificationTemporaryDeadline": chosen_exam.get('certificationTemporaryDeadline', 'false'),
            "certificationShipped": chosen_exam.get('certificationShipped', 14),
            "certificationPrefix": chosen_exam.get('certificationPrefix', ''),
            "licenseExpirationDate": chosen_exam.get('licenseExpirationDate', ''),
            "renewText": chosen_exam.get('renewText', 'none'),
            "renewTextInclusion": chosen_exam.get('renewTextInclusion', 'none'),
            "renewLesson": chosen_exam.get('renewLesson', 'none'),
            "renewLessonInclusion": chosen_exam.get('renewLessonInclusion', 'none'),
            "revisionLawInformation": chosen_exam.get('revisionLawInformation', 'false'),
            
            # --- All Fee Types Inherited from Parent Exam ---
            "examFee": chosen_exam.get('examFee', '0'),
            "studentFee": chosen_exam.get('studentFee', ''),
            "groupFee": chosen_exam.get('groupFee', ''),
            "lessonFee": chosen_exam.get('lessonFee', ''),
            "certificationFee": chosen_exam.get('certificationFee', ''),
            "renewalFee": chosen_exam.get('renewalFee', ''),
            "renewTextFee": chosen_exam.get('renewTextFee', ''),
            "renewLessonFee": chosen_exam.get('renewLessonFee', ''),
            "specialFee": chosen_exam.get('specialFee', ''),

            # --- Derived from Venues ---
            "prefectures": prefectures,
            "examHoldPlace": exam_hold_places,

            # --- Schedule-Specific Dates & Times ---
            "examHoldDate": exam_hold_date.isoformat(),
            "applicationPeriodFrom": application_from_date.isoformat(),
            "applicationPeriodTo": application_to_date.isoformat(),
            "resultDay": result_date.isoformat(),
            "downloadPermissionDate": (exam_hold_date - timedelta(days=7)).isoformat(),
            "certificationPeriodTo": (result_date + timedelta(days=14)).isoformat(),
            "startTime": start_time_obj.strftime("%H:%M"),
            "openTime": (start_time_obj - timedelta(minutes=30)).strftime("%H:%M"),
            "lessonStartTime": (start_time_obj - timedelta(minutes=60)).strftime("%H:%M"),
            "lessonOpenTime": (start_time_obj - timedelta(minutes=90)).strftime("%H:%M"),


            # --- Schedule-Specific Status & Overrides ---
            "examHoldActivation": random.choice(["true", "false"]),
            "renewal": "false",
            "renewalReserve": "false",
            "documentsRequired": ["写真付き身分証明書"], # Example override
            "cautions": ["会場内での飲食はご遠慮ください。", "試験開始後の入室は認められません。"],
            "memo": f"第{i+1}回 {chosen_exam['examName']} の試験日程です。"
        }
        schedules.append(schedule)

    # Add standard fields and serialize for DynamoDB
    serializer = TypeSerializer()
    final_records = []
    for record in schedules:
        record["partitionKey"] = "EXAM_HOLD"
        record["sortKey"] = str(uuid.uuid4())
        record["createdBy"] = created_by_user
        record["createdOn"] = now_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        record["updatedBy"] = created_by_user
        record["updatedOn"] = now_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        dynamodb_item = {k: serializer.serialize(v) for k, v in record.items() if v or isinstance(v, (list, bool, int, float))}
        final_records.append(dynamodb_item)

    return final_records

def batch_insert_records(records_to_insert, batch_size=25):
    """Inserts records into DynamoDB using BatchWriteItem."""
    total_records = len(records_to_insert)
    put_requests = [{"PutRequest": {"Item": item}} for item in records_to_insert]
    print(f"\n📨 Starting batch insert of {total_records} schedule records...")

    for i in range(0, len(put_requests), batch_size):
        batch = put_requests[i:i + batch_size]
        try:
            response = dynamodb_client.batch_write_item(RequestItems={TABLE_NAME: batch})
            success_count = len(batch)
            if 'UnprocessedItems' in response and response['UnprocessedItems']:
                unprocessed_count = len(response['UnprocessedItems'].get(TABLE_NAME, []))
                success_count -= unprocessed_count
                print(f"  ⚠️ {unprocessed_count} items were not processed.")
            print(f"Processed batch of {len(batch)}. Succeeded: {success_count}.")
        except Exception as e:
            print(f"❌ An exception occurred during batch insert: {e}")
        time.sleep(1)

# --- Main execution ---
if __name__ == "__main__":
    print(f"🚀 Starting script to insert mock exam schedules into table '{TABLE_NAME}'...")
    
    all_exams = get_full_items_by_pk("EXAM")
    all_venues = get_full_items_by_pk("EXAM_PLACE")

    if all_exams and all_venues:
        schedule_data_list = create_mock_schedule_data(all_exams, all_venues)
        batch_insert_records(schedule_data_list)
        print("\n✅ All exam schedule data insertion attempts complete.")
    else:
        print("\n❌ Script stopped. Cannot create schedules without existing exams AND venues to link.")