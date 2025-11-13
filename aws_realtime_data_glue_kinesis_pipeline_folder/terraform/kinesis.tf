resource "aws_kinesis_stream" "flight_stream" {
    name = var.kinesis_stream_name
    shard_count = var.shard_count
}