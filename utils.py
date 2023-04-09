import pandas as pd
from enum import Enum

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
