import boto3
import json

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = "your-s3-bucket-name"
    file_name = "ingested_data.json"
    
    # Example: Fetching data (can replace with an API call)
    data = [
        {"id": 1, "feature1": 10.5, "feature2": 20.3, "label": 1},
        {"id": 2, "feature1": 14.2, "feature2": 19.6, "label": 0},
    ]
    
    # Upload to S3
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(data),
    )
    return {"statusCode": 200, "body": "Data ingested successfully"}
