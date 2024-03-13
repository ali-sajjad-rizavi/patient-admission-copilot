import os
import json
import boto3
from urllib.parse import unquote_plus


OUTPUT_BUCKET_NAME = "admission-copilot-facesheets"
OUTPUT_S3_PREFIX = "textract-responses"  # Folder where the response goes

# To tell which topic should be notified when textract job completes.
SNS_TOPIC_ARN = "<paste topic ARN here>"
# The role which allows 'AmazonSNSFullAccess' and 'AmazonTextractServiceRole' permissions
# so that textract is authorized to send notification on SNS.
SNS_ROLE_ARN = "<paste SNS role here>"


def lambda_handler(event, context):
    textract = boto3.client("textract")
    
    total_records = len(event["Records"])
    
    # Sometimes, when lots of PDF get uploaded at a specific time range, they
    # are batched together and sent in the same event.
    failed_jobs_count = 0
    for record in event["Records"]:
        bucketname = str(event["Records"][0]["s3"]["bucket"]["name"])
        filename = unquote_plus(str(event["Records"][0]["s3"]["object"]["key"]))
    
        print(f"Bucket: {bucketname} ::: Key: {filename}")
    
        response = textract.start_document_text_detection(
            DocumentLocation={
                "S3Object": {"Bucket": bucketname, "Name": filename}
            },
            OutputConfig={
                "S3Bucket": OUTPUT_BUCKET_NAME, "S3Prefix": OUTPUT_S3_PREFIX
            },
            NotificationChannel={
                "SNSTopicArn": SNS_TOPIC_ARN, "RoleArn": SNS_ROLE_ARN
            },
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            failed_jobs_count += 1
        else:
            print("Job ID:", response["JobId"])
    
    if failed_jobs_count:
        return {
            "statusCode": 200,
            "body": json.dumps(
                f"{failed_jobs_count} jobs were failed out of {total_records}!"
            )
        }
    
    return {
        "statusCode": 200, "body": json.dumps("Job(s) created successfully!")
    }
