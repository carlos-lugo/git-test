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

def create_hardcoded_venue_data():
    """
    Creates a list of 10 hardcoded mock exam venue records.
    This version includes the number of available seats in 'placeCapacity'.
    """
    # Use a single timestamp for all records for consistency
    now_utc = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    created_by_user = "system_seed_script"

    venues = [
        {
            "placeName": "TKPガーデンシティPREMIUM東京駅", "prefecture": "東京都", "city": "中央区八重洲1-5-9",
            "addressLine": "八重洲MTビル 5F", "building": "八重洲MTビル", "postalCode": "103-0028", "phoneNumber": "03-3527-9971",
            "placeCapacity": "120"
        },
        {
            "placeName": "梅田スカイビル 会議室", "prefecture": "大阪府", "city": "大阪市北区大淀中1-1-88",
            "addressLine": "タワーウエスト 22F", "building": "梅田スカイビル", "postalCode": "531-6023", "phoneNumber": "06-6440-3901",
            "placeCapacity": "250"
        },
        {
            "placeName": "TKP札幌駅カンファレンスセンター", "prefecture": "北海道", "city": "札幌市北区北7条西2-9",
            "addressLine": "ベルヴュオフィス札幌 2F/3F", "building": "ベルヴュオフィス札幌", "postalCode": "060-0807", "phoneNumber": "011-700-2121",
            "placeCapacity": "80"
        },
        {
            "placeName": "JR博多シティ 会議室", "prefecture": "福岡県", "city": "福岡市博多区博多駅中央街1-1",
            "addressLine": "JR博多シティ 10F", "building": "JR博多シティ", "postalCode": "812-0012", "phoneNumber": "092-431-1381",
            "placeCapacity": "150"
        },
        {
            "placeName": "名古屋コンベンションホール", "prefecture": "愛知県", "city": "名古屋市中村区平池町4-60-12",
            "addressLine": "グローバルゲート 3F", "building": "グローバルゲート", "postalCode": "453-6103", "phoneNumber": "052-589-8000",
            "placeCapacity": "300"
        },
        {
            "placeName": "パシフィコ横浜 会議センター", "prefecture": "神奈川県", "city": "横浜市西区みなとみらい1-1-1",
            "addressLine": "会議センター 3F", "building": "パシフィコ横浜", "postalCode": "220-0012", "phoneNumber": "045-221-2155",
            "placeCapacity": "200"
        },
        {
            "placeName": "仙台AER 展望テラス", "prefecture": "宮城県", "city": "仙台市青葉区中央1-3-1",
            "addressLine": "AER 31F", "building": "AERビル", "postalCode": "980-6131", "phoneNumber": "022-724-1111",
            "placeCapacity": "75"
        },
        {
            "placeName": "広島コンベンションホール", "prefecture": "広島県", "city": "広島市東区二葉の里3-5-4",
            "addressLine": "広テレビル 1F", "building": "広島テレビ・ビッグフロント広島", "postalCode": "732-0057", "phoneNumber": "082-261-3311",
            "placeCapacity": "180"
        },
        {
            "placeName": "神戸国際会館セミナーハウス", "prefecture": "兵庫県", "city": "神戸市中央区御幸通8-1-6",
            "addressLine": "セミナーハウス 8F", "building": "神戸国際会館", "postalCode": "651-0087", "phoneNumber": "078-231-8161",
            "placeCapacity": "60"
        },
        {
            "placeName": "沖縄コンベンションセンター", "prefecture": "沖縄県", "city": "宜野湾市真志喜4-3-1",
            "addressLine": "会議棟A", "building": "沖縄コンベンションセンター", "postalCode": "901-2224", "phoneNumber": "098-898-3000",
            "placeCapacity": "220"
        }
    ]

    # Add standard fields to each venue record
    for record in venues:
        record["partitionKey"] = "EXAM_PLACE"
        record["sortKey"] = str(uuid.uuid4())
        record["createdBy"] = created_by_user
        record["createdOn"] = now_utc
        record["updatedBy"] = created_by_user
        record["updatedOn"] = now_utc
        # Add other fields from the schema as empty strings if not present
        record.setdefault("memo", "")
        record.setdefault("placeUrl", "")

    return venues

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
    print(f"Preparing to insert hardcoded exam venues into table '{TABLE_NAME}'...")

    # Generate the list of 10 venue records
    venue_data_list = create_hardcoded_venue_data()
    number_of_records_to_generate = len(venue_data_list)

    print(f"Generated {number_of_records_to_generate} venue records with capacity. Starting batch insert...")

    # Insert the records into DynamoDB
    batch_insert_records(venue_data_list, number_of_records_to_generate)

    print("All venue data insertion attempts complete.")