import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io import ReadFromText, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition, CreateDisposition
import csv
import io
import logging
from datetime import datetime
import argparse

class ParseCSVRow(beam.DoFn):
    """Parse CSV row and convert data types according to BigQuery schema"""
    
    def __init__(self, skip_header=True):
        self.skip_header = skip_header
        self.header_skipped = False
    
    def process(self, element):
        # Skip header row
        if self.skip_header and not self.header_skipped:
            self.header_skipped = True
            return
        
        try:
            # Parse CSV row
            reader = csv.reader(io.StringIO(element))
            row = next(reader)
            
            # Expected number of columns based on schema
            expected_columns = 32
            if len(row) != expected_columns:
                logging.warning(f"Row has {len(row)} columns, expected {expected_columns}. Skipping row: {element}")
                return
            
            # Convert data types according to BigQuery schema
            record = {
                'first_name': row[0] if row[0] else None,
                'last_name': row[1] if row[1] else None,
                'phone': row[2] if row[2] else None,
                'phone_number_hashed': row[3] if row[3] else None,
                'email_hashed': row[4] if row[4] else None,
                'google_advertiser_id__gaid_': row[5] if row[5] else None,
                'apple_id_for_advertisers__idfa_': row[6] if row[6] else None,
                'consent_data': row[7] if row[7] else None,
                'consent_timestamp': self._parse_timestamp(row[8]),
                'date_of_birth': self._parse_date(row[9]),
                'city': row[10] if row[10] else None,
                'gender': row[11] if row[11] else None,
                'state': row[12] if row[12] else None,
                'source': row[13] if row[13] else None,
                'what_is_your_favourite_brand_': row[14] if row[14] else None,
                'how_often_do_you_consume_biscuits_': row[15] if row[15] else None,
                'are_you_a_parent': self._parse_boolean(row[16]),
                'liked_recipes': row[17] if row[17] else None,
                'isscan': row[18] if row[18] else None,
                'zip': row[19] if row[19] else None,
                'packtype': row[20] if row[20] else None,
                'chocolate_consumption': row[21] if row[21] else None,
                'created_story': row[22] if row[22] else None,
                'jio_gems_cricket_answer': row[23] if row[23] else None,
                'cadbury_brand': row[24] if row[24] else None,
                'mithai_consumption_flag': self._parse_boolean(row[25]),
                'candies_consumption_flag': self._parse_boolean(row[26]),
                'biscuit_consumption_flag': self._parse_boolean(row[27]),
                'birth_year': self._parse_integer(row[28]),
                'coupon_flag': row[29] if row[29] else None,
                # Extra columns for error testing - these will be ignored by BigQuery if not in schema
                'nick_name': row[30] if len(row) > 30 and row[30] else None,
                'religion': row[31] if len(row) > 31 and row[31] else None,
            }
            
            yield record
            
        except Exception as e:
            logging.error(f"Error parsing row: {element}. Error: {str(e)}")
            # You can choose to skip bad rows or handle them differently
            return
    
    def _parse_boolean(self, value):
        """Convert string boolean to actual boolean"""
        if value is None or value == '':
            return None
        if isinstance(value, bool):
            return value
        if value.lower() in ['true', '1', 'yes']:
            return True
        elif value.lower() in ['false', '0', 'no']:
            return False
        else:
            return None
    
    def _parse_integer(self, value):
        """Convert string to integer"""
        if value is None or value == '':
            return None
        try:
            return int(value)
        except ValueError:
            return None
    
    def _parse_date(self, value):
        """Parse date string to proper format"""
        if value is None or value == '':
            return None
        try:
            # Assuming format is YYYY-MM-DD
            datetime.strptime(value, '%Y-%m-%d')
            return value
        except ValueError:
            logging.warning(f"Invalid date format: {value}")
            return None
    
    def _parse_timestamp(self, value):
        """Parse timestamp string to proper format"""
        if value is None or value == '':
            return None
        try:
            # Remove 'UTC' suffix if present and parse
            clean_value = value.replace(' UTC', '')
            dt = datetime.strptime(clean_value, '%Y-%m-%d %H:%M:%S')
            return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        except ValueError:
            logging.warning(f"Invalid timestamp format: {value}")
            return None

def get_bigquery_schema():
    """Define BigQuery table schema"""
    return [
        {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'phone', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'phone_number_hashed', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'email_hashed', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'google_advertiser_id__gaid_', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'apple_id_for_advertisers__idfa_', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'consent_data', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'consent_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'date_of_birth', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'gender', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'what_is_your_favourite_brand_', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'how_often_do_you_consume_biscuits_', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'are_you_a_parent', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'liked_recipes', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'isscan', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'zip', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'packtype', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'chocolate_consumption', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created_story', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'jio_gems_cricket_answer', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'cadbury_brand', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'mithai_consumption_flag', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'candies_consumption_flag', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'biscuit_consumption_flag', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'birth_year', 'type': 'INT64', 'mode': 'NULLABLE'},
        {'name': 'coupon_flag', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]

def run_pipeline(argv=None):
    """Run the Apache Beam pipeline"""
    
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_file',
        dest='input_file',
        required=True,
        help='Input CSV file path (local or GCS)')
    parser.add_argument(
        '--output_table',
        dest='output_table',
        required=True,
        help='Output BigQuery table in format project:dataset.table')
    parser.add_argument(
        '--project',
        dest='project',
        required=True,
        help='GCP Project ID')
    parser.add_argument(
        '--temp_location',
        dest='temp_location',
        required=True,
        help='GCS temp location for Dataflow')
    parser.add_argument(
        '--staging_location',
        dest='staging_location',
        required=True,
        help='GCS staging location for Dataflow')
    parser.add_argument(
        '--runner',
        dest='runner',
        default='DirectRunner',
        help='Pipeline runner (DirectRunner for local, DataflowRunner for cloud)')
    parser.add_argument(
        '--region',
        dest='region',
        default='us-central1',
        help='GCP region for Dataflow')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Set up pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    
    # Configure Google Cloud options
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = known_args.project
    google_cloud_options.temp_location = known_args.temp_location
    google_cloud_options.staging_location = known_args.staging_location
    google_cloud_options.region = known_args.region
    
    # Configure standard options
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.runner = known_args.runner
    
    # Create and run pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Read CSV file
        csv_data = (
            pipeline
            | 'Read CSV' >> ReadFromText(known_args.input_file)
        )
        
        # Parse and transform data
        parsed_data = (
            csv_data
            | 'Parse CSV Rows' >> beam.ParDo(ParseCSVRow())
        )
        
        # Write to BigQuery
        _ = (
            parsed_data
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=known_args.output_table,
                schema=get_bigquery_schema(),
                create_disposition=CreateDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                additional_bq_parameters={
                    'ignoreUnknownValues': True,  # This will ignore extra columns like nick_name and religion
                    'maxBadRecords': 100  # Allow some bad records
                }
            )
        )
        
        logging.info(f"Pipeline completed. Data written to {known_args.output_table}")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()