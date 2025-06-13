# file_metadata_tracker/config.py

# Resource names (logical IDs)
NEW_S3_BUCKET_NAME = "FileUploadBucket"
S3_BUCKET_TO_TRACK = 'simpletest01'

# lambda function name
FILE_META_DATA_PROCESSOR_LAMBDA = 'FileMetaDataProcessor'
# FILE_META_DATA_ROLE = ''

#dynamo db tables
LATEST_TABLE_NAME = "FileMetadataLatest"
SKIPPED_TABLE_NAME   = "FileMetadataSkipped"
FAILED_TABLE_NAME    = "FileMetadataFailed"
HISTORY_TABLE_NAME   = "FileMetadataHistory"
DELETED_TABLE_NAME = "FileDeleted"

# env vars
ENV_LATEST_TABLE = "FILE_METADATA_LATEST"
ENV_SKIPPED_TABLE   = "FILE_METADATA_SKIPPED"
ENV_FAILED_TABLE    = "FILE_METADATA_FAILED"
ENV_HISTORY_TABLE   = "FILE_METADATA_HISTORY"
ENV_DELETED_TABLE = "FILE_DELETED"

