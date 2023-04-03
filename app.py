import os
import csv
import boto3
from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

sqs = boto3.client(
    'sqs',
    region_name=os.environ.get('AWS_REGION'),
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
)

# Get the Elasticsearch host and port from environment variables
es_host = os.environ.get('ES_HOST', 'elasticsearch')
es_port = os.environ.get('ES_PORT', '9200')

# Define the Elasticsearch client
es = Elasticsearch(hosts=[f"http://{es_host}:{es_port}"])

queue_url = os.environ.get('SQS_QUEUE_URL')

def send_csv_to_elasticsearch(sqs_message_body, elasticsearch_host, elasticsearch_port, index_name):
    # Parse the CSV data using the csv module
    rows = []
    for row in csv.reader(sqs_message_body.splitlines()):
        rows.append(row)

    # Transform the CSV data into a list of dictionaries (one for each row)
    headers = rows[0]
    data = []
    for row in rows[1:]:
        data.append({header: value for header, value in zip(headers, row)})

    # Create a connection to Elasticsearch
    es = Elasticsearch(hosts=[{"host": elasticsearch_host, "port": elasticsearch_port}])

    # Index the data into an Elasticsearch index
    for i, doc in enumerate(data):
        es.index(index=index_name, body=doc, id=i)

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    file = request.files['file']
    if file:
        filename = file.filename
        file.save(filename)
        send_csv_to_sqs(filename)
        return jsonify({'message': 'File uploaded successfully.'})
    else:
        return jsonify({'error': 'No file was provided.'})

@app.route('/consume_csv', methods=['GET'])
def consume_csv():
    response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    messages = response.get('Messages', [])
    if messages:
        message = messages[0]
        csv_data = message['Body']
        send_csv_to_elasticsearch(csv_data, es_host, es_port, "csv_data")
        # You can do something with the rows here
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
        return jsonify({'message': 'CSV file consumed successfully.'})
    else:
        return jsonify({'message': 'No CSV file to consume.'})

def send_csv_to_sqs(filename):
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        rows = []
        for row in reader:
            rows.append(row)
            # You can do something with each row if needed
        response = sqs.send_message(QueueUrl=queue_url, MessageBody=str(rows))
        print(response['MessageId'])

if __name__ == '__main__':
    app.run(port=5000)
