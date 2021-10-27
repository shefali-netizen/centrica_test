from os import getenv

EXECUTION_DATE = getenv('EXECUTION_DATE', None)
DATABASE_RAW = getenv('DATABASE', 'datalake_raw')
TABLE_RESPONSE_DATA = getenv('TABLE_RESPONSE_DATA', 'job_boards_response_data_v0')
TABLE_DUPLICATES = getenv('TABLE_DUPLICATES', 'job_boards_duplicates_v0')
S3_DUPLICATES = getenv(
    'S3_DUPLICATES',
    's3://profinda-datalake-stg/datalake-raw/job-boards/response-duplicates-tracking/v0')
AWS_REGION = getenv('AWS_REGION', 'eu-west-1')
STAGING_BUCKET = getenv('STAGING_BUCKET', 's3://aws-athena-query-results-765330055732-eu-west-1')
