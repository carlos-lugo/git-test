import boto3
import time

# --- Configuration ---
# Set your DynamoDB table name and AWS profile
TABLE_NAME = "fossy_stg"
AWS_PROFILE_NAME = "asdf"
PARTITION_KEY_TO_DELETE = "EXAM_HOLD" # The partition key of the records to be deleted
# ---------------------

# AWS Setup
print(f"Using AWS Profile: {AWS_PROFILE_NAME}")
session = boto3.Session(profile_name=AWS_PROFILE_NAME)
dynamodb_client = session.client('dynamodb', region_name='ap-northeast-1')

def get_keys_to_delete(partition_key):
    """
    Scans the DynamoDB table to find all primary keys (partitionKey and sortKey)
    for items with the specified partition key. Handles pagination.
    """
    print(f"🔍 Searching for all items with partitionKey = '{partition_key}'...")
    keys = []
    last_evaluated_key = None

    while True:
        query_args = {
            'TableName': TABLE_NAME,
            'ProjectionExpression': 'partitionKey, sortKey',
            'KeyConditionExpression': 'partitionKey = :pk',
            'ExpressionAttributeValues': {':pk': {'S': partition_key}}
        }
        if last_evaluated_key:
            query_args['ExclusiveStartKey'] = last_evaluated_key

        try:
            response = dynamodb_client.query(**query_args)
            keys.extend(response.get('Items', []))
            last_evaluated_key = response.get('LastEvaluatedKey', None)
            if not last_evaluated_key:
                break # Exit loop if no more pages
        except Exception as e:
            print(f"❌ An error occurred while fetching keys: {e}")
            return [] # Return empty list on error
    
    return keys

def batch_delete_records(keys_to_delete, batch_size=25):
    """
    Deletes records from DynamoDB in batches using BatchWriteItem.
    """
    total_to_delete = len(keys_to_delete)
    successful_deletes = 0
    
    # Format keys for BatchWriteItem's DeleteRequest
    delete_requests = [{"DeleteRequest": {"Key": key}} for key in keys_to_delete]

    print(f"\n🗑️ Starting batch deletion of {total_to_delete} records...")
    for i in range(0, len(delete_requests), batch_size):
        batch = delete_requests[i:i + batch_size]
        try:
            response = dynamodb_client.batch_write_item(RequestItems={TABLE_NAME: batch})
            
            success_in_batch = len(batch)
            if 'UnprocessedItems' in response and response['UnprocessedItems']:
                unprocessed_count = len(response['UnprocessedItems'].get(TABLE_NAME, []))
                success_in_batch -= unprocessed_count
                print(f"  ⚠️ {unprocessed_count} items were not processed and should be retried.")

            successful_deletes += success_in_batch
            print(f"Processed batch. Succeeded: {success_in_batch}. (Total Deleted: {successful_deletes}/{total_to_delete})")

        except Exception as e:
            print(f"❌ An exception occurred during batch delete: {e}")
        
        time.sleep(1) # Pause between batches to respect throughput

# --- Main execution ---
if __name__ == "__main__":
    print(f"🚀 Starting script to remove mock exam schedules from table '{TABLE_NAME}'...")
    
    # 1. Find all items that need to be deleted
    keys = get_keys_to_delete(PARTITION_KEY_TO_DELETE)
    
    if not keys:
        print(f"✅ No records found with partitionKey '{PARTITION_KEY_TO_DELETE}'. Nothing to do.")
    else:
        print(f"Found {len(keys)} records to delete.")
        
        # 2. SAFETY CHECK: Confirm with the user before deleting
        confirm = input(f"Are you sure you want to permanently delete these {len(keys)} records? (type 'yes' to confirm): ")
        
        if confirm.lower() == 'yes':
            # 3. If confirmed, proceed with deletion
            batch_delete_records(keys)
            print("\n✅ All deletion attempts complete.")
        else:
            print("\n🛑 Operation cancelled by user. No records were deleted.")