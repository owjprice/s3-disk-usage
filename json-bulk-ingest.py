#!/usr/bin/python
import pprint
import json
import argparse

# from services.elasticsearch import ElasticSearchIndex, ElasticSearchFactory
# from conf.elasticsearch_mapper import daily_bucket_storageclass_size_mapping

from elasticsearch import Elasticsearch
from elasticsearch import helpers

# ------------------------
# Config Items
# ------------------------
ELASTICSEARCH_HOST = 'localhost'
ELASTICSEARCH_PORT = 9200
INDEX_NAME = 'daily_bucket_storageclass_sizes'
DOC_TYPE = 'daily_bucket_storageclass_size'
# ------------------------

pp = pprint.PrettyPrinter(indent=4)

es = Elasticsearch([{'host': ELASTICSEARCH_HOST, 'port': ELASTICSEARCH_PORT}])

def bulk_ingest(inputfile):
    with open(inputfile) as json_file:  
        data = json.load(json_file)
        for item in data:
            yield {
                "_index"  : INDEX_NAME,
                "_type"   : DOC_TYPE,
                "_source" : item
            }
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='JSON File ingestor for S3 Inventory')
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('-i', '--input', help='Input file name', required=True)
    args = parser.parse_args()
    helpers.bulk(es, bulk_ingest(args.input))