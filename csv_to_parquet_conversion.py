import pyarrow.csv as pv
import pyarrow.parquet as pq

csv_path = "SMS Engagement Data.csv"
parquet_path = "sms_engagement.parquet"

convert_options = pv.ConvertOptions(
    timestamp_parsers=["%Y-%m-%d %H:%M:%S"]
)

table = pv.read_csv(
    csv_path,
    convert_options=convert_options
)

pq.write_table(
    table,
    parquet_path,
    compression="snappy"
)

print("Parquet file written:", parquet_path)
