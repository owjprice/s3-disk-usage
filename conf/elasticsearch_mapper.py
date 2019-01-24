s3object_mapping = {
    "settings": {
        "number_of_shards": 3,
        "number_of_replicas": 2
    },
    "s3object": {
        "properties": {
            "polling_timestamp": {"type": "date"},
            "last_modified": {"type": "date"},
            "version_id": {"type": "string"},
            "e_tag": {"type": "string"},
            "storage_class": {"type": "string"},
            "key": {"type": "string"},
            "owner": {
                "display_name" : {"type": "string"},
                "id": {"type": "string"},
            },
            "is_latest": {"type": "boolean"},
            "size": {"type": "bytes"},
            "id": {"type": "string"},
            "bucket_name": {"type": "string"}
        }
    }
}
