import boto3
import json
from elasticsearch import Elasticsearch
from datetime import datetime


s3 = boto3.client('s3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)


es = Elasticsearch("http://localhost:9200")

def transform_and_index(bucket_name):
    response = s3.list_objects_v2(Bucket=bucket_name)
    for obj in response.get("Contents", []):
        key = obj["Key"]
        file_obj = s3.get_object(Bucket=bucket_name, Key=key)
        raw_data = json.loads(file_obj["Body"].read())

        # Vérification des données
        if "id" in raw_data and "title" in raw_data:
            document = {
                "id": raw_data.get("id"),
                "title": raw_data.get("title"),
                "content": raw_data.get("text", ""),
                "url": raw_data.get("url", ""),
                "score": raw_data.get("score", 0),
                "timestamp": datetime.utcfromtimestamp(raw_data.get("time", 0)).isoformat()
            }

            es.index(index="hackernews", doc_type="_doc", id=document["id"], body=document)
            print(f" Document {document['id']} inséré.")
        else:
            print(f" Données invalides dans {key}")

if __name__ == "__main__":
    transform_and_index("raw")
