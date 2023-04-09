import csv
import json
from elasticsearch import Elasticsearch
import re


def get_property_keys(index_name , hosts):
    # Initialize Elasticsearch client
    es = Elasticsearch(hosts=['http://elastic.reachiq.io:80'])

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
    es = Elasticsearch(hosts=['http://elastic.reachiq.io:80'])

    # Read in the CSV file and convert it to a JSON array
    # with open(csv_file_path, 'r') as file:
    csv_reader = csv.DictReader(file)
    json_data = json.dumps([row for row in csv_reader])    

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

        # Add the documents to Elasticsearch
        # print(json.dumps(company_document, indent=4))
        # print(json.dumps(record_document, indent=4))
        es.index(index='primary_company_list_data', body=company_document)
        es.index(index='primary_record_list_data', body=record_document)


# Define the mapping arrays for each index
company_index_name = "primary_company_list_data"
company_mapping_array = get_property_keys(company_index_name)
company_filtered_list = [x for x in company_mapping_array if not re.match(r'^[\d@]', x)]

# print(company_mapping_array)


primary_record_index_name = "primary_record_list_data"
record_mapping_array = get_property_keys(primary_record_index_name)
record_filtered_list = [x for x in record_mapping_array if not re.match(r'^[\d@]', x)]

print(record_mapping_array)

# Call the function to add the data to Elasticsearch
add_data_to_elasticsearch('./contactData.csv', company_filtered_list, record_filtered_list)
