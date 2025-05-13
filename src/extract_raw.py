import boto3
import requests
import argparse
import json
from time import sleep

s3 = boto3.client('s3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='root',
    aws_secret_access_key='root',
    region_name='us-east-1'
)

BUCKET = "raw"

TOP_STORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
ITEM_URL = "https://hacker-news.firebaseio.com/v0/item/{}.json"

def fetch_top_story_ids(limit):
    response = requests.get(TOP_STORIES_URL)
    response.raise_for_status()
    return response.json()[:limit]

def fetch_story_details(story_id):
    url = ITEM_URL.format(story_id)
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def upload_to_s3(data, key):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(data))

def main(limit):
    print(f"Extraction des {limit} articles HackerNews dans le bucket S3 '{BUCKET}'...")
    story_ids = fetch_top_story_ids(limit)

    for i, story_id in enumerate(story_ids):
        try:
            data = fetch_story_details(story_id)
            key = f"{story_id}.json"
            upload_to_s3(data, key)
            print(f"Article {i+1}/{limit} uploadé : {key}")
            sleep(0.5)
        except Exception as e:
            print(f" Erreur pour l'article {story_id} : {e}")

if __name__ == "__main__":
    print('Script started')

    import sys
    sys.argv = [
        "extract_raw.py",
        "--limit", "50",
    ]
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()
    main(args.limit)
