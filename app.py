import os
import csv
import boto3
from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
from dotenv import load_dotenv
from io import BytesIO, TextIOWrapper
import pandas as pd
from utils import OperationType, generate_mapping_from_csv, get_property_keys, add_data_to_elasticsearch, update_index_docs, csv_to_json,upsert_index_docs
from pymongo import MongoClient
import json
import re

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
es_url = f"http://{es_host}:{es_port}"
print(es_url)

# Define the Elasticsearch client
es = Elasticsearch(hosts=[es_url])

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

@app.route('/upload_data', methods=['POST'])
def upload():
    try:
        body = request.data
        # convert the bytes object to a string
        body_str = body.decode('utf-8')
        # convert the string to a dictionary
        body_dict = json.loads(body_str)
        # Get request parameters
        op_type = OperationType[body_dict.get('type')]
        auto_fill = body_dict.get('auto_fill', False)
        # Get the message from the SQS queue
        message_id = body_dict.get('message_id')
        response = sqs.receive_message(QueueUrl=queue_url, MessageAttributeNames=['All'])
        # Extract the CSV file from the message
        # Check if there are any messages in the queue
        messages = response.get('Messages')
        if not messages:
            return {'message': 'No messages in the queue.'}

        # Process the first message
        message = messages[0]
        body = message.get('Body', '')
        receipt_handle = message.get('ReceiptHandle', {})
        auto_fill = body_dict.get('auto_fill', False)

        # Define the mapping arrays for each index
        company_index_name = "primary_company_list_data"
        company_mapping_array = get_property_keys(company_index_name, es_url)
        company_filtered_list = [x for x in company_mapping_array if not re.match(r'^[\d@]', x)]
        primary_record_index_name = "primary_record_list_data"
        record_mapping_array = get_property_keys(primary_record_index_name, es_url)
        record_filtered_list = [x for x in record_mapping_array if not re.match(r'^[\d@]', x)]

        # Define switcher function
        def create():
            get_records = add_data_to_elasticsearch(body, company_filtered_list, record_filtered_list, es_url, True)
            if(get_records.get('success', False)):
                # mongo_db[company_index_name].insert_many(get_records["company_docs"])
                # mongo_db[primary_record_index_name].insert_many(get_records["record_docs"])
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )

        def update():
            get_records = add_data_to_elasticsearch(body, company_filtered_list, record_filtered_list, es_url, False)
            if all([
                    update_index_docs(get_records["company_docs"], company_index_name, "company_website", "company_website.keyword", es_url),
                    update_index_docs(get_records["record_docs"], primary_record_index_name, "email", "email.keyword", es_url)
            ]):
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )




        def create_or_update():
            get_records = add_data_to_elasticsearch(body, company_filtered_list, record_filtered_list, es_url, False)
            if all([
                    upsert_index_docs(get_records["company_docs"], company_index_name, "company_website", "company_website.keyword", es_url),
                    upsert_index_docs(get_records["record_docs"], primary_record_index_name, "email", "email.keyword", es_url)
            ]):
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )

        # Call switcher function based on operation type
        switcher = {
            OperationType.CREATE: create,
            OperationType.UPDATE: update,
            OperationType.UPSERT: create,
        }
        switcher[op_type]()
        # Return success response
        return {'message': 'Data uploaded successfully.'}
            
    except Exception as e:
        # Return error response
        return {'message': str(e)}, 500
                


if __name__ == '__main__':
    app.run(port=5000)
