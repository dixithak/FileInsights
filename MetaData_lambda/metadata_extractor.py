import boto3
from datetime import datetime
import os
import pyarrow.parquet as pq
import io
from io import BytesIO
import zipfile
import gzip

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

main_table = dynamodb.Table(os.environ['FILE_METADATA_LATEST'])
skipped_table = dynamodb.Table(os.environ['FILE_METADATA_SKIPPED'])
failed_table = dynamodb.Table(os.environ['FILE_METADATA_FAILED'])
history_table = dynamodb.Table(os.environ['FILE_METADATA_HISTORY'])


def detect_delimiter(line):
    for delim in [',', '\t', ';', '|']:
        if delim in line:
            return delim
    return ','

def parse_header_line(raw_line):
    try:
        line = raw_line.decode('utf-8').strip()
    except UnicodeDecodeError:
        return ["Decode error"]
    delimiter = detect_delimiter(line)
    return [col.strip().strip('"') for col in line.split(delimiter)]

def read_file_header(file_data, key):
    try:
        # Handle Parquet files
        if key.endswith('.parquet'):
            table = pq.read_table(BytesIO(file_data))
            return table.schema.names

        # Handle ZIP files
        elif key.endswith('.zip'):
            with zipfile.ZipFile(BytesIO(file_data)) as zipf:
                names = [f for f in zipf.namelist() if f.endswith('.csv') or f.endswith('.txt')]
                if not names:
                    return ["No CSV or TXT file in ZIP"]
                with zipf.open(names[0]) as csvfile:
                    return parse_header_line(csvfile.readline())

        # Handle GZ, CSV, TXT
        elif key.endswith('.gz') or key.endswith('.csv') or key.endswith('.txt') or key.endswith('.psv'):
            file_like = gzip.GzipFile(fileobj=BytesIO(file_data)) if key.endswith('.gz') else BytesIO(file_data)
            with file_like as f:
                return parse_header_line(f.readline())

        else:
            return ["Unsupported file format"]

    except Exception as e:
        print(f"Header parsing error for {key}: {e}")
        return [f"Header parsing failed: {str(e)}"]



def metadata_handler(event, context):
    try:
        bucket = event["detail"]["bucket"]["name"]
        key = event["detail"]["object"]["key"]
        filepath = f"{bucket}/{key}"
        timestamp = datetime.utcnow().isoformat()
        print(key)

        try:
            response = s3.head_object(Bucket=bucket, Key=key)
            
        except Exception as e:
            print(f"FAILED: {filepath} | Error: {str(e)}")
            failed_table.put_item(Item={
                'filepath': filepath,
                'bucket': bucket,
                'timestamp': timestamp,
                'error': str(e)
            })
            return

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
            'timestamp': timestamp,
            'header' : header,
            'column_count' : column_count
        }

        if key.endswith('/') or size == 0:
            print(f"SKIPPED: {filepath}")
            skipped_table.put_item(Item={**metadata, 'reason': 'folder-like or empty object'})
            return

        try:
            # Download the full file content
            obj = s3.get_object(Bucket=bucket, Key=key)
            file_data = obj['Body'].read()

            # Extract headers based on file type
            metadata['header'] = read_file_header(file_data, key)
            metadata['column_count'] = len(metadata['header'])

        except Exception as e:
            print(f"HEADER PARSE FAILED: {filepath} | Error: {str(e)}")

            failed_table.put_item(Item={**metadata, 'reason': 'Header parsing Issue'})
            return



        # Check if file already exists
        existing = main_table.get_item(Key={'filepath': filepath})

        if 'Item' in existing:
            # Archive old version to history table
            old_item = existing['Item']
            history_table.put_item(Item={
                **old_item,
                #'timestamp': timestamp  # Timestamp is the sort key in history table
            })

        # Store new/updated metadata
        print(f"STORED: {filepath}")
        main_table.put_item(Item=metadata)

    except Exception as e:
        print(f"Error in metadata_handler: {e}")
        raise e
