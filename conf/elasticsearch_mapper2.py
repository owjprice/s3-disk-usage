daily_bucket_storageclass_size_mapping = {
    "settings": {
        "number_of_shards": 3,
        "number_of_replicas": 2
    },
    "s3object": {
        "properties": {
            "bucket_name": {"type": "string"},
            "storage_class": {"type": "string"},
            "date": {"type": "date"},
            "total_size": {"type": "bytes"},
            "owner": {
                "display_name": {"type": "string"},
                "id": {"type": "string"}
            }
        }
    }
}
