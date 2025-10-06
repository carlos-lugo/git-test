import boto3
import time

# --- Configuration ---
TABLE_NAME = "fossy_stg"
USERNAMES_TO_KEEP = ["carlos", "carlos-admin"]
# If you configured a specific AWS profile in your aws cli, add it here.
# Otherwise, boto3 will use your default credentials.
AWS_PROFILE_NAME = "asdf" # IMPORTANT: Replace with your AWS profile name
session = boto3.Session(profile_name=AWS_PROFILE_NAME)
dynamodb_client = session.client('dynamodb', region_name='ap-northeast-1')
# --------------------

def delete_unwanted_items(limit=None):
    """
    Scans a DynamoDB table and deletes items except for those with usernames
    specified in USERNAMES_TO_KEEP.

    If a limit is provided, the scan will stop as soon as enough items are found.

    Args:
        limit (int, optional): The maximum number of items to delete.
                               If None, all found items will be deleted. Defaults to None.
    """
    items_to_delete = []
    
    paginator = dynamodb_client.get_paginator('scan')
    
    filter_expression = "NOT ({})".format(
        ' OR '.join([f'username = :u{i}' for i in range(len(USERNAMES_TO_KEEP))])
    )
    expression_attribute_values = {
        f':u{i}': {'S': username} for i, username in enumerate(USERNAMES_TO_KEEP)
    }

    print(f"Scanning table '{TABLE_NAME}' to find items to delete...")
    
    page_iterator = paginator.paginate(
        TableName=TABLE_NAME,
        ProjectionExpression="partitionKey, sortKey", 
        FilterExpression=filter_expression,
        ExpressionAttributeValues=expression_attribute_values
    )

    # --- OPTIMIZATION: Loop with early exit for efficiency ---
    stop_scan = False
    for page in page_iterator:
        time.sleep(2)
        for item in page.get('Items', []):
            delete_request = {
                'DeleteRequest': {
                    'Key': {
                        'partitionKey': item['partitionKey'],
                        'sortKey': item['sortKey']
                    }
                }
            }
            items_to_delete.append(delete_request)
            
            # If a limit is set, check if we've found enough items to stop scanning
            if limit is not None and len(items_to_delete) >= limit:
                print(f"Found {len(items_to_delete)} items, which meets the limit of {limit}. Stopping scan.")
                stop_scan = True
                break # Exit the inner loop (over items in the page)
        
        if stop_scan:
            break # Exit the outer loop (over pages)
    # --- End of Optimization ---

    if not items_to_delete:
        print("No items found to delete. All relevant items are in the keep list.")
        return

    # Ensure we only delete the exact number requested, as the last page
    # might have contained more items than needed.
    if limit is not None:
        items_to_delete = items_to_delete[:limit]
    
    total_to_delete = len(items_to_delete)
    print(f"Preparing to delete {total_to_delete} items. Starting batch deletion...")

    batch_size = 25
    total_deleted_count = 0 

    for i in range(0, total_to_delete, batch_size):
        batch = items_to_delete[i:i + batch_size]
        try:
            response = dynamodb_client.batch_write_item(
                RequestItems={
                    TABLE_NAME: batch
                }
            )
            
            total_deleted_count += len(batch) 
            print(f"  Successfully deleted batch of {len(batch)} items. (Total: {total_deleted_count}/{total_to_delete})")
            
            if 'UnprocessedItems' in response and response['UnprocessedItems']:
                print(f"  WARNING: Could not process {len(response['UnprocessedItems'])} items. You may need to run the script again.")

        except Exception as e:
            print(f"Error deleting batch: {e}")
            
    print("\nDeletion process complete.")

# --- Main execution (No changes needed here) ---
if __name__ == "__main__":
    confirm = input(f"This will delete items from '{TABLE_NAME}' except where username is in {USERNAMES_TO_KEEP}.\nHave you backed up your table? (yes/no): ")
    
    if confirm.lower() != 'yes':
        print("Operation cancelled.")
    else:
        deletion_limit = None
        while True:
            mode = input("Do you want to delete all matching items or a specific amount? (Enter 'all' or 'amount'): ").lower()
            if mode == 'all':
                break
            elif mode == 'amount':
                while True:
                    try:
                        amount_str = input("How many items do you want to delete? ")
                        deletion_limit = int(amount_str)
                        if deletion_limit > 0:
                            break
                        else:
                            print("Please enter a number greater than 0.")
                    except ValueError:
                        print("Invalid input. Please enter a whole number.")
                break
            else:
                print("Invalid choice. Please enter 'all' or 'amount'.")
        
        delete_unwanted_items(limit=deletion_limit)