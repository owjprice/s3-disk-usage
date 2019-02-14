#!/usr/bin/python
import boto3
import pprint
import datetime
import json

from services.elasticsearch import ElasticSearchIndex, ElasticSearchFactory
from conf.elasticsearch_mapper import daily_bucket_storageclass_size_mapping

# ------------------------
# Config Items
# ------------------------
ASSUME_ROLE_ARN = ''
ASSUME_ROLE_SESSION_NAME = ''
ASSUME_ROLE_EXTERNAL_ID = ''
ASSUME_ROLE_DURATION = 43200

ELASTICSEARCH_HOST = 'localhost'
ELASTICSEARCH_PORT = 9200
# ------------------------

sts_client = boto3.client('sts')
assumed_role_object=sts_client.assume_role(
    RoleArn=ASSUME_ROLE_ARN,
    RoleSessionName=ASSUME_ROLE_SESSION_NAME,
    ExternalId=ASSUME_ROLE_EXTERNAL_ID,
    DurationSeconds=ASSUME_ROLE_DURATION
)
credentials=assumed_role_object['Credentials']
s3=boto3.resource(
    's3',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken'],
)
cloudwatch=boto3.resource(
    'cloudwatch',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken'],
)
pp = pprint.PrettyPrinter(indent=4)


indexer = ElasticSearchIndex(
            ElasticSearchFactory(
                ELASTICSEARCH_HOST,
                ELASTICSEARCH_PORT,
            ),
            'daily_bucket_storageclass_sizes',
            'daily_bucket_storageclass_size',
            daily_bucket_storageclass_size_mapping
        )

storageTypes = [
    {
        'StorageType': 'StandardStorage',
        'StorageClass': 'STANDARD'
    },
    {
        'StorageType': 'IntelligentTieringStorage',
        'StorageClass': 'INTELLIGENT_TIERING'
    },
    {
        'StorageType': 'StandardIAStorage',
        'StorageClass': 'STANDARD_IA'
    },
    {
        'StorageType': 'OneZoneIAStorage',
        'StorageClass': 'ONEZONE_IA'
    },
    {
        'StorageType': 'ReducedRedundancyStorage',
        'StorageClass': 'REDUCED_REDUNDANCY'
    },
    {
        'StorageType': 'GlacierStorage',
        'StorageClass': 'GLACIER'
    }
]
metric = cloudwatch.Metric('AWS/S3', 'BucketSizeBytes')
startTime = datetime.datetime.combine(datetime.date.today(), datetime.time())
endTime = datetime.datetime.combine(datetime.date.today()+datetime.timedelta(1), datetime.time())

def metric_crawler():
    to_be_written = []
    for bucket in s3.buckets.all():
        to_be_indexed = {}
        print('Crawling bucket: ' + bucket.name)
        # Make a call to get the Average total storage per storage class for a 24 hour period.
        # We will call this once daily.
        for item in storageTypes:
            response = metric.get_statistics(
                Dimensions=[
                    {
                        'Name': 'BucketName',
                        'Value': bucket.name
                    },
                    {
                        'Name': 'StorageType',
                        'Value': item['StorageType']
                    }
                ],
                StartTime=startTime,
                EndTime=endTime,
                Period=86400,
                Statistics=['Average'],
                Unit='Bytes'
            )
            for data in response['Datapoints']:

                # pp.pprint(data['Timestamp'].isoformat())

                to_be_indexed['bucket_name'] = bucket.name
                to_be_indexed['storage_class'] = item['StorageClass']
                to_be_indexed['date'] = data['Timestamp'].isoformat()
                to_be_indexed['total_size'] = data['Average']
                to_be_indexed['owner'] = {
                    'display_name': bucket.Acl().owner['DisplayName'],
                    'id': bucket.Acl().owner['ID']
                }
                to_be_written.append(to_be_indexed)
                # pp.pprint(to_be_written)
    with open('result.json', 'w') as fp:
        json.dump(to_be_written, fp)
        

if __name__ == '__main__':
    metric_crawler()