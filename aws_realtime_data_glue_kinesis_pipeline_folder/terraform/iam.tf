resource "aws_iam_role" "glue_role" {
  name = "glue_service_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "glue.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_policy" "kinesis_put_policy" {
  name        = "kinesis_put_policy"
  description = "Policy to allow Glue put records to Kinesis stream"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "kinesis:PutRecord",
          "kinesis:PutRecords"
        ],
        Resource = "arn:aws:kinesis:${var.aws_region}:${data.aws_caller_identity.current.account_id}:stream/${var.kinesis_stream_name}"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_kinesis_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.kinesis_put_policy.arn
}

data "aws_caller_identity" "current" {}
