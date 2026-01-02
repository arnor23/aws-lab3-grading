import json
import os
import boto3

sqs = boto3.client("sqs")
QUEUE_URL = os.environ["QUEUE_URL"]

def lambda_handler(event, context):
    for record in event["Records"]:
        key = record["s3"]["object"]["key"]

        if not key.lower().endswith((".jpg", ".jpeg", ".png")):
            continue

        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps({
                "bucket": record["s3"]["bucket"]["name"],
                "key": key,
                "etag": record["s3"]["object"]["eTag"]
            })
        )
