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

from datetime import date
from utils.common_util import (
    render_template,
    get_secret
)

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from turbodbc import connect, make_options



def create_mssql(sql_file, path, key, db_conn_secret_name,**kwargs):
    
    # Establish AWS connection 
    
    session = boto3.session.Session(profile_name="aws_profile_name", )
    s3_client = session.client("s3", use_ssl=False)
    
    print("using session:",session)
    
    # Get credentials from AWS Secrets Manager and create SQL Server connection string
    
    options     = make_options(prefer_unicode=True)
    mssql_con   = get_secret(db_conn_secret_name)
    user        = mssql_con.get("user")
    user_pass   = mssql_con.get("user_pass")
    server      = mssql_con.get("server")
    port        = mssql_con.get("port")
    database    = mssql_con.get("database")
    driver      = mssql_con.get("driver")
    
    
    print("conn info from SM:",user,server,database,driver)
    print("MSSQL Server name:",db_conn_secret_name)
    
    # Establish SQL Server ODBC connection 
    
    con = turbodbc.connect(
        turbodbc_options=options,
        driver=driver,
        server=server,
        database=database,
        UID=user,
        PWD=user_pass,
        )
   
    
    # Create load date for S3 folder partitioning
    # To do - make this dynamic
    load_date = date.today().strftime("%Y-%m-%d")
    
    # Render templatized SQL statement
    sql_as_string = render_template(sql_file, path, database = database) #, database=database)
   
    # Execute rendered SQL against MS SQL connection and load into dataframe
    df = pd.io.sql.read_sql(sql_as_string, con)

    csv_buffer = io.StringIO()
    
    # Write dataframe to csv
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    gz_buffer = io.BytesIO()
    
    # stream and upload csv as a gzip to S3 

    with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
        gz_file.write(bytes(csv_buffer.getvalue(), 'utf-8'))
    try:
        s3_client.put_object(Bucket='s3-bucket-name', Key=f"""{key}/{db_conn_secret_name}_{load_date}.csv.gz""", Body=gz_buffer.getvalue())
        return_code = 0
    except Exception as e:
        return_code = 1
        print(e)
    return return_code

if __name__ == "__main__":
    f"""
    Function to Run export from MSSQL to S3
    Valid Arguments are sql_file key db_conn_secret_name
    """

    sql_file = sys.argv[1]
    path = sys.argv[2]
    key = sys.argv[3]
    db_conn_secret_name = sys.argv[4]
    


    return_code = create_mssql(sql_file, path, key, db_conn_secret_name)

    if return_code == 0:
        sys.exit(0)
    else:
        raise SystemError(f"Error {return_code}")
