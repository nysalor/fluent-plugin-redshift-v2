Amazon Redshift output plugin for Fluentd
========

## Overview

Amazon Redshift output plugin uploads event logs to an Amazon Redshift Cluster. Supported data formats are csv, tsv and json. An S3 bucket and a Redshift Cluster are required to use this plugin. Forked from fluent-plugin-redshift, and re-implemented with aws-sdk-v2 and fluentd 0.14.


## Installation

    fluent-gem install fluent-plugin-redshift-v2

## Configuration

Format:

    <match my.tag>
        type redshift_v2

        # s3 (for copying data to redshift)
        aws_key_id YOUR_AWS_KEY_ID
        aws_sec_key YOUR_AWS_SECRET_KEY
        ## or Use IAM Role instead of credentials.
        aws_iam_role arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME

        s3_bucket YOUR_S3_BUCKET
        s3_endpoint YOUR_S3_BUCKET_END_POINT
        path YOUR_S3_PATH
        timestamp_key_format year=%Y/month=%m/day=%d/hour=%H/%Y%m%d-%H%M
        s3_server_side_encryption S3_SERVER_SIDE_ENCRYPTION

        # redshift
        redshift_host YOUR_AMAZON_REDSHIFT_CLUSTER_END_POINT
        redshift_port YOUR_AMAZON_REDSHIFT_CLUSTER_PORT
        redshift_dbname YOUR_AMAZON_REDSHIFT_CLUSTER_DATABASE_NAME
        redshift_user YOUR_AMAZON_REDSHIFT_CLUSTER_USER_NAME
        redshift_password YOUR_AMAZON_REDSHIFT_CLUSTER_PASSWORD
        redshift_schemaname YOUR_AMAZON_REDSHIFT_CLUSTER_TARGET_SCHEMA_NAME
        redshift_tablename YOUR_AMAZON_REDSHIFT_CLUSTER_TARGET_TABLE_NAME
        redshift_copy_columns COLMUNS_FOR_COPY
        file_type [tsv|csv|json|msgpack]

        # buffer
        buffer_type file
        buffer_path /var/log/fluent/redshift
        flush_interval 15m
		retry_wait 30s
		retry_limit 5
        buffer_chunk_limit 1g
		buffer_queue_limit 100
		flush_at_shutdown true
		num_threads 4
    </match>

+ `type` (required) : The value must be `redshift_v2`.

+ `aws_key_id` : AWS access key id to access s3 bucket.

+ `aws_sec_key` : AWS secret key id to access s3 bucket.

+ `aws_iam_role` : AWS IAM Role name to access s3 bucket and copy into redshift.

+ `s3_bucket` (required) : s3 bucket name. S3 bucket must be same as the region of your Redshift cluster.

+ `s3_endpoint` : s3 endpoint.

+ `path` (required) : s3 path to input.

+ `timestamp_key_format` : The format of the object keys. It can include date-format directives.

  - Default parameter is "year=%Y/month=%m/day=%d/hour=%H/%Y%m%d-%H%M"
  - For example, the s3 path is as following with the above example configration.
    <pre>
  logs/example/year=2013/month=03/day=05/hour=12/20130305_1215_00.gz
  logs/example/year=2013/month=03/day=05/hour=12/20130305_1230_00.gz
</pre>

+ `s3_server_side_encryption` : S3 Server-Side Encryption (Only aes256 is supported)

+ `redshift_host` (required) : the end point(or hostname) of your Amazon Redshift cluster.

+ `redshift_port` (required) : port number.

+ `redshift_dbname` (required) : database name.

+ `redshift_user` (required) : user name.

+ `redshift_password` (required) : password for the user name.

+ `redshift_tablename` (required) : table name to store data.

+ `redshift_schemaname` : schema name to store data. By default, this option is not set and find table without schema as your own search_path.

+ `redshift_connect_timeout` : maximum time to wait for connection to succeed.

+ `redshift_copy_columns` : columns for copying. Value needs to be comma-separated like `id,name,age`

+ `file_type` : file format of the source data.  `csv`, `tsv`, `msgpack` or `json` are available.

+ `delimiter` : delimiter of the source data. This option will be ignored if `file_type` is specified.

+ `buffer_type` : buffer type.

+ `buffer_path` : path prefix of the files to buffer logs.

+ `flush_interval` : flush interval.

+ `buffer_chunk_limit` : limit buffer size to chunk.

+ `buffer_queue_limit` : limit buffer files to chunk.

+ `num_threads` : number of threads to load data to redshift.

+ `utc` : utc time zone. This parameter affects `timestamp_key_format`.

and standard buffered output options. (see https://docs.fluentd.org/v0.14/articles/buffer-plugin-overview)

## Logging examples
```ruby
# examples by fluent-logger
require 'fluent-logger'
log = Fluent::Logger::FluentLogger.new(nil, host: 'localhost', port: 24224)

# file_type: csv
log.post('your.tag', log: "12345,12345")

# file_type: tsv
log.post('your.tag', log: "12345\t12345")

# file_type: json
require 'json'
log.post('your.tag', { user_id: 12345, data_id: 12345 }.to_json)

# file_type: msgpack
log.post('your.tag', user_id: 12345, data_id: 12345)
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
