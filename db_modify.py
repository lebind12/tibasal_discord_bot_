import boto3
import os


access_key = os.environ["access_key"]
secret_access_key = os.environ["secret_access_key"]
region = os.environ["region"]

session = boto3.Session(
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_access_key,
    region_name = region
)
client = session.client('dynamodb')

def get_data():
    result = []
    res = client.scan(
        TableName="crawl_data"
    )
    if len(res["Items"]) == 0:
        return result
    else:
        for data in res["Items"]:
            url = data["url"]["S"]
            name = data["name"]["S"]
            result.append([name, url])
    return result
if __name__ == "__main__":
    print(get_data())