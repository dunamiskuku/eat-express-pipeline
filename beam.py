import re
import apache_beam as beam
from google.cloud import bigquery
import argparse
from apache_beam.options.pipeline_options import PipelineOptions

# Define Arguments
parser = argparse.ArgumentParser()
parser.add_argument('--input', dest='input', required=True, help='Input file to process')
paths_args, pipeline_args = parser.parse_known_args()

input_file = paths_args.input 

delivered_table_spec = 'eat-express-511605:eat_express_dataset.delivered_orders'
others_table_spec = 'eat-express-511605:eat_express_dataset.others_status_orders'

# Initalize BigQuery
client = bigquery.Client()
dataset_id = 'eat-express-511605:eat_express_dataset'

try:
    client.get_dataset(dataset_id)
except Exception as e:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = 'US'
    dataset_description = 'Dataset for food orders'
    client.create_dataset(dataset_id, exists_ok=True)

def remove_last_column(item):
    if item.endswith(','):
        return item[:-1]
    return item

def process_row(row):
    columns = row.split(',')
    if len(columns) > 4:
        columns[4] = remove_last_column(columns[4])
    return ','.join(columns)

def remove_special_characters(row):
    columns = row.split(',')
    ret = ''
    for col in columns:
        ret += re.sub(r'[^a-zA-Z0-9]', '', col) + ','
    return ret[:-1]

def print_row(row):
    print(row)

def to_json(row):
    fields = row.split(',')
    return {
        'Customer_id': fields[0],
        'date': fields[1],
        'timestamp': fields[2],
        'order_id': fields[3],
        'items': fields[4],
        'amount': fields[5],
        'mode': fields[6],
        'restaurant': fields[7],
        'Status': fields[8],
        'ratings': fields[9],
        'feedback': fields[10],
        'new_column': fields[11]
    }

table_schema = '''
    Customer_id: STRING,	
    date: STRING,	
    timestamp: STRING,	
    order_id: STRING,	
    items: STRING,	
    amount: STRING,	
    mode: STRING,	
    restaurant: STRING,	
    Status: STRING,	
    ratings: STRING,	
    feedback: STRING,
    new_column: STRING
'''
options = PipelineOptions(pipeline_args)

with beam.Pipeline(options=options) as p:
    cleaned_data = (
        p
        | 'Read Input File' >> beam.io.ReadFromText(input_file, 
                                                    skip_header_lines=1)
        | 'Process Items Column' >> beam.Map(process_row)
        | 'Convert to Lowercase' >> beam.Map(lambda row: row.lower())
        | 'Remove Special Characters' >> beam.Map(remove_special_characters)
        | 'Add Count Column' >> beam.Map(lambda row: row + ',1')
    )

    delivered_orders = (
        cleaned_data
        | 'Filter Delivered Orders' >> beam.Filter(lambda row: row.split(',')[8].lower() == 'delivered')
    )
    undelivered_orders = (
        cleaned_data
        | 'Filter Undelivered Orders' >> beam.Filter(lambda row: row.split(',')[8].lower() != 'delivered')
    )

(
    delivered_orders
    | 'Delivered to JSON' >> beam.Map(to_json)
    | 'Write Delivered Orders to BigQuery' >> beam.io.WriteToBigQuery(
        delivered_table_spec,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
    )
)

(
    undelivered_orders
    | 'Undelivered to JSON' >> beam.Map(to_json)
    | 'Write Undelivered Orders to BigQuery' >> beam.io.WriteToBigQuery(
        others_table_spec,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
    )
)