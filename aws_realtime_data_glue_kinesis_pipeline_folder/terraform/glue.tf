resource "aws_glue_catalog_database" "flight_db" {
  name = var.glue_catalog_name
}

resource "aws_glue_job" "flight_transform" {
  name     = var.glue_job_name
  role_arn = aws_iam_role.glue_role.arn

  command {
    name          = "glueetl"
    script_location = "s3://${var.aws_s3_glue_script_bucket_name}/scripts/glue_script.py"
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"                       = "s3://${var.aws_s3_glue_script_bucket_name}/glue/"
    "--job-language"                  = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--extra-jars"      = "s3://${var.aws_s3_glue_script_bucket_name}/jars/spark-streaming-kinesis-asl_2.13-3.3.0"
  }
}

