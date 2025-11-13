# Global
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-north-1"
}

variable "aws_s3_raw_data_bucket_name" {
  description = "S3 bucket for raw data"
  type        = string
}

variable "aws_s3_log_bucket_name" {
  description = "S3 bucket for log data"
  type        = string
}

variable "aws_s3_glue_script_bucket_name" {
  description = "S3 bucket for glue script"
  type        = string
}

variable "aws_s3_landed_flight_bucket_name" {
  description = "S3 bucket for landed flights"
  type        = string
}

variable "aws_s3_processed_data_bucket_name" {
  description = "S3 bucket for processed data"
  type        = string
}

# PostgreSQL (RDS)
variable "db_instance_identifier" {
  description = "RDS PostgreSQL instance identifier"
  type        = string
}

variable "db_username" {
  description = "RDS PostgreSQL master username"
  type        = string
}

variable "db_password" {
  description = "PostgreSQL password from Secrets Manager"
  type        = string
  sensitive   = true
}

variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.micro"
}

variable "db_allocated_storage" {
  description = "RDS allocated storage in GB"
  type        = number
  default     = 20
}

variable "glue_catalog_name" {
  description = "glue catalog name"
  type = string
}

variable "glue_job_name" {
  description = "glue job name"
  type = string
}

variable "kinesis_stream_name" {
  description = "kinesis stream name"
  type = string
}

variable "shard_count" {
  description = "shard_count"
  type = number
}

