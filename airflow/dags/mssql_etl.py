from datetime import datetime, timedelta, date
from boto3 import Session
import logging
import airflow
import os
import json

from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

from utils.snowflake import (
    dag_failure_callback,
    start_load,
    end_load,
    execute_templated_sql,
    execute_s3_copy_into_snowflake,
    last_update,
)

from utils.common_util import (
    ecs_context,
    ecs_register_task,
    ecs_run_task,
    ecs_task_status,
    ecs_exec_task,
    get_params,
    s3_context,
    get_secret,
    get_aws_session
)
from utils.mssql import (
    create_mssql
)

__version__ = "1"
__dag_name__ = "srd_am_mssql"
__dag_folder__ = __dag_name__  # Defaults to Dag but can be overriden

# Get Parameters - specify the dag specific params here
dag_params = get_params(__dag_folder__, __version__, ["mssql"])

# ecs Connection
ecs_client = ecs_context("aws_conn")
s3_client, s3_resource = s3_context("aws_conn")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_date = date.today().strftime("%Y-%m-%d")

args = {
    "owner": "audience-measurement",
    "start_date": datetime(2020, 5, 1),
    "load_date": load_date,
    "params": dag_params,
    "depends_on_past": False,
}


with DAG(
    dag_id=f"{__dag_name__}_V{__version__}",
    schedule_interval="0 * * * *",
    catchup=False,
#     on_success_callback=dag_success_slack_alert,
#      on_failure_callback=task_failure_pagerduty_incident,
    default_args=args,
    params=dag_params
    
) as dag:
    start = PythonOperator(
        task_id="START_LOAD",
        python_callable=start_load,
        provide_context=True,
        op_kwargs={}
    )
    end = DummyOperator(task_id = 'end_load')



    
