resource "aws_security_group" "glue_sg" {
  name        = "glue-security-group"
  description = "Allow Glue/Lambda to access AWS resources"
  vpc_id      = aws_vpc.main.id

  # Allow internal communication within the security group (optional)
  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  # Allow all outbound traffic (S3, internet, etc.)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
