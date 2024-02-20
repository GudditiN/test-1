pip install firebase
pip install stream-chat
pip install boto3

import os
import firebase_admin
from firebase_admin import credentials, firestore
from stream_chat import StreamChat
from collections import Counter
import pandas as pd
import csv
import boto3
from datetime import datetime

# Read Firebase credentials JSON path from environment variable
firebase_credentials_json = os.getenv("FIREBASE_CREDENTIALS_JSON")
if firebase_credentials_json is None:
    raise ValueError("Firebase credentials JSON not found in environment variables")

# Initialize Firebase Admin with the credentials
cred = credentials.Certificate(firebase_credentials_json)
firebase_admin.initialize_app(cred)

db = firestore.client()
client = StreamChat("4j2fckwgpzwq", "snu9e5jvw3ec89wh8brgqj8ts5pa888ff4t4kzpf288c4rp4e6te4nudmy9n8tg8")

def get_doc(id):
    document = db.collection("projects").document(id)
    return document

def get_paginated_messages_counts(channel):
    from datetime import date, datetime
    start_date = date.today()
    # start_date = datetime(2023, 11, 10).date() #YYYY-MM-DD

    last_message_id = ''
    len_filtered_messages = 0
    len_messages = 0
    page = 1
    while True:
        result = channel.query(messages={
                        "limit": 300,
                        "offset": (page - 1) * 300
                        })
        try:
            page += 1
            last_message_id = result['messages'][0]['id']
            data = result['messages']
            filtered_data = [item for item in data if start_date == datetime.strptime(item['user']['created_at'], '%Y-%m-%dT%H:%M:%S.%fZ').date()]
            len_filtered_messages += len(filtered_data)
            len_messages += len(result['messages'])
        except Exception as e:
            break

    return {"total_messages": len_messages, "daily_messages": len_filtered_messages}

def create_file(data):
    fields = ["Project_Title", "Channel_Name", "Channel_ID", "DailyMessageCount", "TotalMessagesCount", "MembersCount"]
    with open('output.csv', 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(data)

project_docs = db.collection("projects").stream()
output_data = []
for project_doc in project_docs:
    document = get_doc(project_doc.id)
    channels = document.collection('channels').get()
    for channel in channels:
        channel = client.channel('est-public', channel.id, data={"created_by_id": "admin"})
        res = channel.query()
        message_counts = get_paginated_messages_counts(channel)
        output = {
            "Project_Title": document.get().to_dict().get('title', 'NULL'),
            "Channel_Name": res.get('channel').get('name', 'NULL'),
            "Channel_ID": res.get('channel').get('id', 'NULL'),
            "DailyMessageCount": message_counts['daily_messages'],
            "TotalMessagesCount": message_counts['total_messages'],
            "MembersCount": res['channel'].get('member_count', 0)
        }
        output_data.append(output)

create_file(output_data)

formatted_date_time = datetime.now().strftime("%Y%m%d%H%M%S")

aws_access_key_id = 'AKIAWEEOXF2HAF2SKSC2'
aws_secret_access_key = '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'

s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

local_file_path = 'output.csv'
s3_bucket = 'getstreamtest'
s3_object = f'test/output_{formatted_date_time}.csv'

s3.upload_file(local_file_path, s3_bucket, s3_object)
