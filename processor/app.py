import json
import boto3
from PIL import Image
from io import BytesIO

s3 = boto3.client("s3")

def lambda_handler(event, context):
    for record in event["Records"]:
        body = json.loads(record["body"])
        bucket = body["bucket"]
        key = body["key"]

        metadata_key = key.replace("incoming/", "metadata/") + ".json"

        try:
            s3.head_object(Bucket=bucket, Key=metadata_key)
            continue
        except:
            pass

        obj = s3.get_object(Bucket=bucket, Key=key)
        image = Image.open(BytesIO(obj["Body"].read()))

        metadata = {
            "format": image.format,
            "width": image.width,
            "height": image.height
        }

        s3.put_object(
            Bucket=bucket,
            Key=metadata_key,
            Body=json.dumps(metadata),
            ContentType="application/json"
        )
