import boto3
import uuid
import pprint
import datetime

from services.elasticsearch import ElasticSearchIndex, ElasticSearchFactory
from conf.elasticsearch_mapper import daily_bucket_storageclass_size_mapping

# boto3.set_stream_logger('', 10)

sts_client = boto3.client('sts')

s3=boto3.resource(
    's3'
)
cloudwatch=boto3.resource(
    'cloudwatch'
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
# startTime = datetime.datetime.combine(datetime.date.today()+datetime.timedelta(1), datetime.time())
# endTime = datetime.datetime.combine(datetime.date.today()+datetime.timedelta(2), datetime.time())

def metric_crawler():

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
                to_be_indexed['bucket_name'] = bucket.name
                to_be_indexed['storage_class'] = item['StorageClass']
                to_be_indexed['date'] = data['Timestamp']
                to_be_indexed['total_size'] = data['Average']
                to_be_indexed['owner'] = {
                    'display_name': bucket.Acl().owner['DisplayName'],
                    'id': bucket.Acl().owner['ID']
                }
                if not indexer.index(to_be_indexed):
                    print('Something went wrong inserting into Elasticsearch')
        

if __name__ == '__main__':
    for i in range(5):
        print(i)
        startTime = datetime.datetime.combine(datetime.date(2018, 12, 31)-datetime.timedelta(i), datetime.time())
        endTime = datetime.datetime.combine(datetime.date(2018, 12, 31)-datetime.timedelta(i-1), datetime.time())
        print(startTime)
        print(endTime)
        metric_crawler()