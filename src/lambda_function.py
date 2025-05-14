import os
import logging
import boto3
import pandas as pd
import io

# Initialize once, outside handler
s3 = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configurable via Lambda environment variables
DEST_BUCKET = os.environ.get('DEST_BUCKET')
DEST_PREFIX = os.environ.get('DEST_PREFIX', '')


def lambda_handler(event, context):
    """
    Lambda handler to convert a CSV file in S3 to Parquet and upload it back to S3.
    Expects an S3 Put event as input.
    """
    try:
        record = event['Records'][0]['s3']
        src_bucket = record['bucket']['name']
        src_key = record['object']['key']
        logger.info(f"Converting s3://{src_bucket}/{src_key} to Parquet")

        # Read CSV into DataFrame
        obj = s3.get_object(Bucket=src_bucket, Key=src_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        logger.info(f"DataFrame loaded: {df.shape[0]} rows, {df.shape[1]} cols")

        # Convert to Parquet in memory
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        # Build destination key
        base = os.path.splitext(src_key.split('/')[-1])[0]
        dest_key = f"{DEST_PREFIX}{base}.parquet"
        dest_bucket = DEST_BUCKET or src_bucket

        # Upload
        s3.put_object(Bucket=dest_bucket, Key=dest_key, Body=buffer.getvalue())
        logger.info(f"Uploaded parquet to s3://{dest_bucket}/{dest_key}")

        return {
            'statusCode': 200,
            'body': f"Converted {src_key} to {dest_key}"
        }

    except Exception as e:
        logger.exception("Error during conversion")
        raise
