from elasticsearch import Elasticsearch
import os
from dotenv import load_dotenv
from data import doc

# Load environment variables
load_dotenv()

# Elasticsearch client
es = Elasticsearch(hosts=[os.getenv('ELASTICSEARCH_URL')])
response = es.count(index="bank_index")
print(f"Total documents in bank_index: {response['count']}")