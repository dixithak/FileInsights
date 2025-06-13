import os
import json
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')

# Environment variables for table names

latest_table = dynamodb.Table(os.environ['FILE_METADATA_LATEST'])
deleted_table = dynamodb.Table(os.environ['FILE_DELETED'])

def handler(event, context):
    try:
        print("Received event:", json.dumps(event))

        bucket = event["detail"]["bucket"]["name"]
        key = event["detail"]["object"]["key"] 
        filepath = f"{bucket}/{key}"
        timestamp = datetime.utcnow().isoformat()

        # Assume S3 event routed in format: {"filepath": "some/key/in/s3"}
        # if 'filepath' not in event:
        #     return {"status": "error", "message": "Missing 'filepath' in event"}

        # filepath = event['filepath']

        # Step 1: Read from FileMetadataLatest
        response = latest_table.get_item(Key={'filepath': filepath})
        item = response.get('Item')

        if not item:
            filename = key.split('/')[-1]
            folder = '/'.join(key.split('/')[:-1])
            compression = None
            if filename.endswith('.gz') or filename.endswith('.tar'):
                compression = filename.split('.')[-1]
                file_type = filename.split('.')[-2] if '.' in filename else 'unknown'
            else:
                file_type = filename.split('.')[-1] if '.' in filename else 'unknown'
            size = response.get('ContentLength', 0)
            content_type = response.get('ContentType', 'unknown')
            column_count = None
            header = None

            metadata = {
            'filepath': filepath,
            'bucket': bucket,
            'folder': folder,
            'filename': filename,
            'file_type': file_type,
            'compression' : compression,
            'size': size,
            'content_type': content_type,
            'timestamp': None,
            'deletion_timestamp': timestamp,
            'header' : header,
            'column_count' : column_count
            }

            deleted_table.put_item(Item={**metadata, 'Comment': 'Deleted file was not tracked'})
            return {"status": "not_found", "message": f"No record found for {filepath}"}
            

        # Step 2: Write to FileDeleted table
        deleted_table.put_item(Item={**item, 'deletion_timestamp': timestamp})

        # Step 3: Delete from FileMetadataLatest
        latest_table.delete_item(Key={'filepath': filepath})
        print(f"Deleted: {filepath}, stored log")


    except ClientError as e:
        print("DynamoDB error:", e.response['Error']['Message'])
        return {"status": "error", "message": str(e)}

    except Exception as e:
        print("Unexpected error:", str(e))
        return {"status": "error", "message": str(e)}
