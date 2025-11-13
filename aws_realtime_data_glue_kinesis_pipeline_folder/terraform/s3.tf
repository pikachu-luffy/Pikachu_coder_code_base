# Raw data bucket for storing incoming flight data
resource "aws_s3_bucket" "raw_data_bucket" {
    bucket = var.aws_s3_raw_data_bucket_name
    force_destroy = true # allows bucket to be deleted even if not empty
}

resource "aws_s3_bucket_versioning" "raw_data_versioning" {
  bucket = aws_s3_bucket.raw_data_bucket.id

  versioning_configuration {
    status = "Enabled" # enables versioning to prevent data loss
  }
}

# Bucket for storing application and Glue logs
resource "aws_s3_bucket" "log_bucket" {
    bucket = var.aws_s3_log_bucket_name
    force_destroy = true
}

resource "aws_s3_bucket_versioning" "log_versioning" {
  bucket = aws_s3_bucket.log_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Bucket for storing Glue ETL scripts
resource "aws_s3_bucket" "glue_script_bucket" {
    bucket = var.aws_s3_glue_script_bucket_name
    force_destroy = true
}

resource "aws_s3_bucket_versioning" "glue_script_versioning" {
  bucket = aws_s3_bucket.glue_script_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Bucket for storing landed/ingested flight data
resource "aws_s3_bucket" "landed_flight_bucket" {
    bucket = var.aws_s3_landed_flight_bucket_name
    force_destroy = true
}

resource "aws_s3_bucket_versioning" "landed_flight_versioning" {
  bucket = aws_s3_bucket.landed_flight_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Bucket for storing processed and transformed data
resource "aws_s3_bucket" "processed_data_bucket" {
    bucket = var.aws_s3_processed_data_bucket_name
    force_destroy = true
}

resource "aws_s3_bucket_versioning" "processed_data_versioning" {
  bucket = aws_s3_bucket.processed_data_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}
