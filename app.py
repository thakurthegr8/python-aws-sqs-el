import os
import csv
import boto3
from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
from dotenv import load_dotenv
from io import BytesIO, TextIOWrapper
import pandas as pd
from utils import OperationType, generate_mapping_from_csv
from pymongo import MongoClient
import json

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
mongodb_url = os.environ.get('MONGO_DB_URL')
mongodb_name = os.environ.get('MONGO_DB_NAME')
# Initialize MongoDB client and database
mongo_client = MongoClient(mongodb_url)
mongo_db = mongo_client[mongodb_name]

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

@app.route('/upload', methods=['POST'])
def upload():
    try:
        # Get request parameters
        identifier = request.args.get('id')
        op_type = OperationType[request.args.get('type')]
        auto_fill = request.args.get('auto_fill')

        # Get the message from the SQS queue
        message_id = request.form.get('message_id')
        response = sqs.receive_message(
                    QueueUrl=queue_url,
                    AttributeNames=['All'],
                    MessageAttributeNames=['All'],
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=0,
                    VisibilityTimeout=0,
                    AttributeNames=[
                        'SentTimestamp'
                    ],
                    MessageAttributeNames=[
                        'All'
                    ],
                    ReceiveRequestAttemptId='string',
                    VisibilityTimeout=123,
                    WaitTimeSeconds=123,
                    ReceiveMessageWaitTimeSeconds=123,
                    AttributeName=['message_id'],
                    AttributeValue=[message_id]
                )
        # Extract the CSV file from the message
        message = response.get('Messages', [])

        if message:
            body = message[0].get('Body', {})
            receipt_handle = message[0].get('ReceiptHandle', {})
            file = BytesIO(body)
            file_wrapper = TextIOWrapper(file, encoding='utf-8')
            index_name = request.form['index_name']
            operation_type = OperationType(request.form.get('type', OperationType.CREATE.value))
            auto_fill = request.form.get('auto_fill', False)

            # Generate the mapping object for the Elasticsearch index
            mapping = generate_mapping_from_csv(file_wrapper, index_name)

            if mapping:
                # Create indices if they do not exist
                for index, index_mapping in mapping.items():
                    if not es_client.indices.exists(index):
                        es_client.indices.create(index=index, body=index_mapping)
                
                # Define switcher function
                def create():
                    for index, data in mapping.items():
                        es_client.index(index=index, body=json.loads(data))
                        mongo_db[index].insert_one(json.loads(data))

                def update():
                    for index, data in mapping.items():
                        for doc in json.loads(data):
                            doc_id = doc.pop('id')
                            try:
                                es_client.update(index=index, id=doc_id, body={'doc': doc})
                                mongo_db[index].update_one({'id': doc_id}, {'$set': doc})
                            except RequestError:
                                pass  # Ignore errors caused by missing documents

                def create_or_update():
                    for index, data in mapping.items():
                        for doc in json.loads(data):
                            doc_id = doc.pop('id')
                            try:
                                es_client.update(index=index, id=doc_id, body={'doc': doc})
                                mongo_db[index].update_one({'id': doc_id}, {'$set': doc})
                            except RequestError:
                                es_client.index(index=index, id=doc_id, body=doc)
                                mongo_db[index].insert_one(doc)

                # Call switcher function based on operation type
                switcher = {
                    OperationType.CREATE: create,
                    OperationType.UPDATE: update,
                    OperationType.CREATE_OR_UPDATE: create_or_update,
                }
                switcher[op_type]()

                # Return success response
                return {'message': 'Data uploaded successfully.'}

            except Exception as e:
                # Return error response
                return {'message': str(e)}, 500


if __name__ == '__main__':
    app.run(port=5000)
