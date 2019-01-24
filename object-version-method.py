import boto3
import uuid
import pprint
from datetime import datetime

from services.elasticsearch import ElasticSearchIndex, ElasticSearchFactory
from conf.elasticsearch_mapper2 import daily_bucket_storageclass_size_mapping

# Create S3 resource
# s3 = boto3.resource('s3')

sts_client = boto3.client('sts')
assumed_role_object=sts_client.assume_role(
    RoleArn='NOTHING_TO_SEE_HERE',
    RoleSessionName='NOTHING_TO_SEE_HERE',
    ExternalId='NOTHING_TO_SEE_HERE',
    DurationSeconds=43200
)
credentials=assumed_role_object['Credentials']
s3=boto3.resource(
    's3',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken'],
)
pp = pprint.PrettyPrinter(indent=4)


indexer = ElasticSearchIndex(
            ElasticSearchFactory(
                'localhost',
                9200,
            ),
            'daily_bucket_storageclass_sizes',
            'daily_bucket_storageclass_size',
            daily_bucket_storageclass_size_mapping
        )

def object_crawler():
    object_counter = 0
    for bucket in s3.buckets.all():
        print('Crawling bucket: ' + bucket.name)
        storage_type_agg = {}
        for s3object in bucket.object_versions.all():
            if s3object.size == None:
                # print('Skipping ' + s3object.object_key)
                continue
            object_counter += 1
            if s3object.storage_class not in storage_type_agg:
                storage_type_agg[s3object.storage_class] = s3object.size
            else:
                try:
                    storage_type_agg[s3object.storage_class] += s3object.size
                except Exception as e:
                    s3obj = {}
                    s3obj['polling_timestamp'] = str(datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z"))
                    s3obj['last_modified'] = s3object.last_modified
                    s3obj['version_id'] = s3object.version_id
                    s3obj['e_tag'] = s3object.e_tag
                    s3obj['storage_class'] = s3object.storage_class
                    s3obj['key'] = s3object.key
                    s3obj['is_latest'] = s3object.is_latest
                    s3obj['size'] = s3object.size
                    s3obj['id'] = str(uuid.uuid4())
                    s3obj['bucket_name'] = s3object.bucket_name
                    pp.pprint(s3obj)
                    raise e

        for storage_class in storage_type_agg:
            to_be_indexed = {
                'bucket_name': bucket.name,
                'storage_class': storage_class,
                'date': str(datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")),
                'total_size': storage_type_agg[storage_class]
            }
            if not indexer.index(to_be_indexed):
                print('Something went wrong inserting into Elasticsearch')
    print(object_counter + ' objects')



        

if __name__ == '__main__':
    object_crawler()