import pandas as pd
from enum import Enum
import csv
import json
from elasticsearch import Elasticsearch
import re
import os
from datetime import datetime

# Function to transform the date string to ISO format
def transform_date_string(obj):
    obj["email_verification_updated_at"] = datetime.strptime(obj["email_verification_updated_at"], '%m/%d/%Y %H:%M').isoformat()
    return obj

class OperationType(Enum):
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    UPSERT = "UPSERT"


def generate_mapping_from_csv(file, index_name):
    # Read the CSV file into a Pandas DataFrame
    df = pd.read_csv(file)

    # Convert the DataFrame columns to an Elasticsearch mapping object
    mapping = {
        "mappings": {
            "properties": {
            }
        }
    }

    for column in df.columns:
        mapping["mappings"]["properties"][column] = {"type": "text"}

    mapping["settings"] = {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }

    mapping["_meta"] = {
        "index_name": index_name
    }

    return mapping


def get_property_keys(index_name,  es_url):
    # Initialize Elasticsearch client
    es = Elasticsearch(hosts=[es_url])

    # Retrieve the mapping object for the index
    mapping = es.indices.get_mapping(index=index_name)
    # Extract the property keys for each field
    property_keys = []
    for field, properties in mapping[index_name]["mappings"].items():
        property_keys.append(list(properties.keys()))

    # Return the property keys
    return property_keys[0]

def add_data_to_elasticsearch(file, company_mapping_array, record_mapping_array, hosts):
    # Connect to Elasticsearch
    es = Elasticsearch(hosts=[hosts])

    # Parse CSV data from the message body
    csv_data = csv.DictReader(file.splitlines())

    # Convert CSV data to a list of dictionaries
    list_of_dicts = [row for row in csv_data]

    # Convert list of dictionaries to a JSON array
    json_data = json.dumps(list_of_dicts) 
    print(json_data) 
    company_docs = []
    record_docs = []
    # Parse the JSON data and insert it into Elasticsearch
    for row in json.loads(json_data):
        # Create a new document for the primary_company_list_data index
        company_document = {}
        for field in row.keys():
            company_document[field] = row[field]

        # Create a new document for the primary_record_list_data index
        record_document = {}
        for field in row.keys():
            record_document[field] = row[field]

        
        company_docs.append(company_document)
        record_docs.append(record_document)

        es.index(index='primary_company_list_data', body=transform_date_string(company_document))
        es.index(index='primary_record_list_data', body=transform_date_string(record_document))

    return { company_docs, record_docs }