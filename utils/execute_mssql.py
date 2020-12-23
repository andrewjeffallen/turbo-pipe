import numpy 
import pandas as pd
import yaml
import os
import turbodbc
import json
import boto3
import io
import gzip
import sys

from botocore.exceptions import ClientError
from datetime import date
from turbodbc import connect, make_options

from common_util import (
    render_template,
    get_aws_session,
    get_secret,
    assign_date_parameters,
    get_transfer_config
)


def mssql_connection(**kwargs):

    options       = make_options(prefer_unicode=True)
    
    """ 
    if you forget to set prefer_unicode when connecting to MS SQL,  you may get anything from garbled up characters or even ODBC error messages 
    """
    
    # get connection string from Secrets Manager and create MS SQL connection
    try:
        mssql_connection = turbodbc.connect(
            turbodbc_options    = options,
            driver              = driver,
            server              = server,   
            database            = database,
            UID                 = username,
            PWD                 = password
    )
        
    except turbodbc.expections.DatabaseError as exception:
        error, = exception.args
        print("MS SQL error code: ", error.code)
        print("MS SQL error message: ", error.message)
        exit(1)
        
    return mssql_connection
    
    print("MS SQL Connection:")
    print(mssql_connection)
    
def execute_templated_sql(**kwargs):
    
    rendered_sql = render_template(**kwargs)
    
    mssql_conn = mssql_connection(**kwargs)
    
    with mssql_conn as conn:
        with conn.cursor() as mssql_cursor:
            
            try: 
                query_results = mssql_cursor.execute(f'{rendered_sql}')
                query_result_list = []
                while True:
                    row = query_results.fetchone()
                    if row is None:
                        break
                    record_list.append(row)
                except turbodbc.DatabaseError as exception:
                    error, = exception.args
                    print("MS SQL error code: ", error.code)
                    print("MS SQL error message: ", error.message)
                    exit(1)
                    
    # Write output in-memory buffer
    csv = io.StringIO()
    
    try:
        csv_writer = csv.writer(
            csv_output,
            delimiter=",",
            quoting=csv.QUOTE_NONNUMERIC,
            lineterminator="\r\n"
        )
        
        csv_writer.writerows(record_list)

    except csv.Error as error:
        print("Error writing CSV output: ", error)
        exit(1)

    return csv_output

def stage_extract_data_s3(extract_data_content, **kwargs):
    
    # Create low-level client
    s3_client = boto3.client("s3")

    date_params = assign_date_parameters(kwargs["execution_ts"])

    print("Date parameters used for target bucket partitioning:")
    print(date_params)

    # Get transfer configuration for upload
    transfer_config = get_transfer_config()

    # Upload file-like object to S3, based on transfer configuration
    s3_client.upload_fileobj(
        io.BytesIO(gzip.compress(extract_data_content.getvalue().encode('utf-8'))),
        Bucket=f'{kwargs["aws_target_bucket"]}',
        Key=f'{kwargs["aws_ingest_prefix"]}/{kwargs["db_schema"]}/{kwargs["db_source"]}/{date_params["year"]}/{date_params["month"]}/{date_params["day"]}/{kwargs["db_source"]}.{kwargs["execution_ts"]}.csv.gz',
        Config=transfer_config
    )

    print('S3 object uploaded.')

if __name__ == "__main__":

    # Create a new argument parser.  Arguments are added using add_argument() in positional order.
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--db_conn_secret_name", help="Name of AWS secret name for the database connection.")
    arg_parser.add_argument("--db_schema", help="Database schema name.")
    arg_parser.add_argument("--db_source", help="Source data structure name.")
    arg_parser.add_argument("--template_file", help="Templated SQL file to be executed.")
    arg_parser.add_argument("--path", help="Path to templated SQL file.")
    arg_parser.add_argument("--aws_target_bucket", help="Target destination bucket to write data to.")
    arg_parser.add_argument("--aws_ingest_prefix", help="Source application/ingestion point for target S3 bucket.")
    arg_parser.add_argument("--execution_ts", help="Logical execution timestamp.")

    args = arg_parser.parse_args()

    # Create dictionary of params
    named_args = {
        "db_conn_secret_name": args.db_conn_secret_name,
        "db_schema": args.db_schema,
        "db_source": args.db_source,
        "template_file": args.template_file,
        "path": args.path,
        "aws_target_bucket": args.aws_target_bucket,
        "aws_ingest_prefix": args.aws_ingest_prefix,
        "execution_ts": args.execution_ts
    }

    print("Command line arguments:")
    print(named_args)

    # Execute templated SQL
    extracted_csv_content = execute_templated_sql(**named_args)

    # Multi-part object upload to S3
    stage_extract_data_s3(
       extracted_csv_content,
       **named_args
    )
