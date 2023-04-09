import boto3
from dotenv import load_dotenv
import os


load_dotenv()


sqs = boto3.client(
    'sqs',
    region_name=os.environ.get('AWS_REGION'),
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
)

queue_url = os.environ.get('SQS_QUEUE_URL')


# Read the CSV file into a string
with open('./contactData.csv', 'r') as f:
    csv_string = f.read()

# Send the CSV string as a message to the queue
response = sqs.send_message(QueueUrl=queue_url,MessageBody=csv_string)
print(response.get('MessageId'))
