import os
import boto3
from datetime import datetime
from firebase_admin import credentials, initialize_app, firestore
from stream_chat import StreamChat
import csv

# Read Firebase credentials JSON path from environment variable
firebase_credentials_json = os.getenv("FIREBASE_CREDENTIALS_JSON")

if firebase_credentials_json is None:
    raise ValueError("Firebase credentials JSON not found in environment variables")

# Initialize Firebase Admin with the credentials
cred = credentials.Certificate(firebase_credentials_json)
initialize_app(cred)

# Access Firestore
db = firestore.client()

# Initialize StreamChat client
client = StreamChat("4j2fckwgpzwq", "snu9e5jvw3ec89wh8brgqj8ts5pa888ff4t4kzpf288c4rp4e6te4nudmy9n8tg8")

def get_doc(id):
    document = db.collection("projects").document(id)
    return document

def get_paginated_messages_counts(channel):
    from datetime import date, datetime
    start_date = date.today()

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

output_data = []
project_docs = db.collection("projects").stream()
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

# Initialize AWS S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id='YOUR_AWS_ACCESS_KEY_ID',
    aws_secret_access_key='YOUR_AWS_SECRET_ACCESS_KEY'
)

# Upload the file to S3 bucket
local_file_path = 'output.csv'
s3_bucket = 'getstreamtest'
formatted_date_time = datetime.now().strftime("%Y%m%d%H%M%S")
s3_object = f'test/output_{formatted_date_time}.csv'
s3.upload_file(local_file_path, s3_bucket, s3_object)
