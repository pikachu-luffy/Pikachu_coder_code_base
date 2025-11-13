aws_region = "eu-north-1"

aws_s3_raw_data_bucket_name = "raw-data-flight-bucket84h674"
aws_s3_log_bucket_name = "raw-data-flight-log-bucket84h674"
aws_s3_glue_script_bucket_name = "raw-data-flight-glue-script-bucket84h674"
aws_s3_landed_flight_bucket_name = "landed-flight-data-bucket84h674"
aws_s3_processed_data_bucket_name = "processed-data-flight-bucket84h674"


# PostgreSQL
db_instance_identifier   = "flight-postgres"
db_username              = "postgres"
# db_password in Secrets Manager
db_instance_class        = "db.t3.micro"
db_allocated_storage     = 20

#glue
glue_catalog_name = "flight_map_catalog"
glue_job_name = "flight_map_glue_job"

#kinesis
kinesis_stream_name = "flight_map_kinesis_stream"
shard_count = 1