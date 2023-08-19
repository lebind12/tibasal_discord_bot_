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
    # 테이블이 존재할 때 까지 대기
    while True:
        res = client.list_tables()
        if len(res["TableNames"]) > 0:
            break
    # 테이블이 정상적으로 생성될 때 까지 대기
    while True:
        res = client.describe_table(
            TableName="crawl_data"
        )
        if res["Table"]["TableStatus"] == "ACTIVE":
            break
    
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
    return sorted(result)
if __name__ == "__main__":
    print(get_data())