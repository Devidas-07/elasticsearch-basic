from elasticsearch import Elasticsearch
import os
from dotenv import load_dotenv

load_dotenv()

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")


def get_es_client():
    """Return a reusable Elasticsearch client configured from environment variables."""
    return Elasticsearch(hosts=[ELASTICSEARCH_URL])
