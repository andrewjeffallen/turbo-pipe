"""
Purpose: Contain & control common utility functions
"""

import boto3
import base64
from botocore.exceptions import ClientError
import jinja2
from dateutil.tz import tzutc
import logging
import re
import os
from collections import ChainMap
import yaml
from datetime import datetime, timedelta
import time

import json

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

#from utils.snowflake import run_query

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_aws_session():

    return boto3.session.Session()

def get_secret(secret_name):

    region_name = "aws-region"

    # Create a Secrets Manager client
    session = get_aws_session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "DecryptionFailureException":
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InternalServiceErrorException":
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InvalidParameterException":
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InvalidRequestException":
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "ResourceNotFoundException":
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
        else:
            secret = base64.b64decode(get_secret_value_response["SecretBinary"])

    return json.loads(secret)



def render_template(template_file, path, **context):
    """
    Renders jinja template

    Parameters:
        object_row (int): the position in the list of objects in the Object yml file
        context["ts"] (datetime): This is the date time of when the dag executed

    Returns:
        rendered_obj: Rendered template (string)
    """
    templateLoader = jinja2.FileSystemLoader(searchpath=f"{path}/")
    templateEnv = jinja2.Environment(loader=templateLoader)
    template = templateEnv.get_template(template_file)
    rendered_obj = template.render(**context)
    logger.info(
        f"""Template Rendered:
        {rendered_obj}"""
    )
    return rendered_obj


def validate_headers(header):
    """
    Determines if the header in the object metadata is True or False

    Returns:
        1 if True, 0 if False, any other type will raise an error
    """
    if header:
        return 1
    elif not header:
        return 0
    else:
        raise print(f"""Missing header logic in yaml file""")


def date_length(date_object):
    if date_object < 10:
        return f"""0{date_object}"""
    else:
        return date_object


def list_new_s3_objects(bucket, prefix, s3_client, **context):
    """
    Creates a list of files that in an S3 prefix

    Parameters:
        prefix (string): the prefix for the file path in the s3 bucket for that object
    """
    kwargs = {"Prefix": prefix}
    while True:
        response = s3_client.list_objects_v2(Bucket=bucket, **kwargs)
        if response["KeyCount"] == 0:
            logger.info(f"""no files in folder {prefix}""")
            break
        for con in response["Contents"]:
            file = con["Key"]
            date = con["LastModified"].replace(tzinfo=tzutc())
            size = con["Size"]
            if file.startswith(prefix):
                # TODO: update to limit search to pattern
                if size > 0:
                    yield file
        try:
            kwargs["ContinuationToken"] = response["NextContinuationToken"]
        except KeyError:
            break


def s3_context(conn_name, **context):
    """
    Defines the S3 context to use. 
    
    Parameters:
        conn_name (string): Pass in the conn name from Airflow.
    """
    session = boto3.session.Session(
        aws_access_key_id=BaseHook.get_connection(conn_name).login,
        aws_secret_access_key=BaseHook.get_connection(conn_name).password,
    )
    s3_client = session.client("s3", use_ssl=False, verify=False)
    s3_resource = session.resource("s3", use_ssl=False, verify=False)

    return s3_client, s3_resource


def get_params(dag_name, version, list=[]):
    """
    Gets the dag parameters to use based on what is sent through.

    Parameters:
        list: A superceding list of parameters that should override global. Last item in list takes precedence.
    """
    # Variables
    dag_params = {}
    dag_params["env"] = os.environ["APP_ENV"]

    list.insert(0, "global")

    for _p in list:

        param = Variable.get(_p, deserialize_json=True)
        for k, v in param.items():
            if isinstance(v, str):
                param[k] = v.format(
                    APP_ENV=os.environ["APP_ENV"],
                    AIRFLOW_HOME=os.environ["AIRFLOW_HOME"],
                    dag_name=dag_name,
                    version=version,
                )
        # Create Single List
        dag_params = {**dag_params, **param}

    return dag_params


def render_yaml(file, **context):
    """
    Takes a YAML file, formats it, and safe loads it
    """
    with open(file) as f:
        template = jinja2.Template(f.read())
        output = template.render(**context)

    return yaml.safe_load(output)


def datetime_range(start, end, delta):
    range = []
    current = start
    while current <= end:
        range.append(current)
        current += delta

    return range


def ecs_context(conn_name, **context):
    """
    Defines the ecs context to use.

    Parameters:
        conn_name (string): Pass in the conn name from Airflow.
    """
    session = boto3.session.Session(
        aws_access_key_id=BaseHook.get_connection(conn_name).login,
        aws_secret_access_key=BaseHook.get_connection(conn_name).password,
        region_name="us-east-1"
    )
    ecs_client = session.client("ecs")

    return ecs_client

def ecs_register_task(ecs_client, family, taskRoleArn, executionRoleArn, image, name, command, aws_logs_group, aws_logs_prefix, **context):
    """
        Defines ecs task

        Parameters:
            ecs_client (string): The aws client with a connection to ecs
            family (string): Must specify a family for a task definition which allows you to track multiple versions of the same task definition. Used as the name for your task definition.
            taskRoleArn (string): The short of full ARN name of the IAM role that containers in this task can assume
            executionRoleArn (string): The ARN of the of the task execution role that grants the ECS container agent permission to make AWS API calls on your behalf
            name (string): The name of a container
            image (string): The image used to start a container

        """
    command = f"""/usr/bin/python3 /usr/local/airflow/{command}"""
    response = ecs_client.register_task_definition(
        family=family,
        taskRoleArn=taskRoleArn,
        executionRoleArn=executionRoleArn,
        networkMode='awsvpc',
        containerDefinitions=[
            {
                "name": name,
                "image": image,
                "essential": True,
                "entryPoint": [
                    "sh",
                    "-c"
                ],
                "command": [
                    command
                ],
                'logConfiguration': {
                    'logDriver': 'awslogs',
                    'options': {
                        'awslogs-group': aws_logs_group,
                        'awslogs-region': 'us-east-1',
                        'awslogs-stream-prefix': aws_logs_prefix
                    }
                },
            },

        ],
        requiresCompatibilities=[
            "FARGATE"
        ],
        cpu="1024",
        memory="2048"
    )
    return response


def ecs_run_task(ecs_client, taskDefinition, cluster, subnets, securityGroups, **context):
    task_response = ecs_client.run_task(
        cluster=cluster,
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [
                    subnets,
                ],
                'securityGroups': [
                    securityGroups,
                ],
            }
        },
        taskDefinition=taskDefinition
    )
    return task_response

def ecs_task_status(ecs_client, cluster, tasks, **context):
    task_desc = ecs_client.describe_tasks(cluster=cluster, tasks=tasks)
    status = task_desc['tasks'][0]['lastStatus']
    while status != 'STOPPED':
        task_desc = ecs_client.describe_tasks(cluster=cluster, tasks=tasks)
        status = task_desc['tasks'][0]['lastStatus']
        time.sleep(3)
    else:
        exit_code = task_desc['tasks'][0]['containers'][0]['exitCode']
        if exit_code == 0:
            print("Success, data loaded to S3")
        else:
            raise print("Failure, check container logs")


def ecs_exec_task(family, name, ecs_client, command, **context):
    taskRoleArn = context['params']['taskRoleArn']
    executionRoleArn = context['params']['executionRoleArn']
    image = context['params']['image']
    cluster = context['params']['cluster']
    securityGroups = context['params']['securityGroups']
    subnets = context['params']['subnets']
    aws_logs_group = context['params']['aws_logs_group']
    aws_logs_prefix = context['params']['aws_logs_prefix']
    print(f"Context Settings Loaded for the DAG: {context}")
    run_date = context["dag_run"].execution_date
    print(run_date)
    response = ecs_register_task(ecs_client, family, taskRoleArn, executionRoleArn, image, name, command, aws_logs_group, aws_logs_prefix)
    revision = response["taskDefinition"]["revision"]
    taskDefinition = f"""{family}:{revision}"""
    task_response = ecs_run_task(ecs_client, taskDefinition, cluster, subnets, securityGroups)
    tasks = [task_response['tasks'][0]['taskArn']]
    ecs_task_status(ecs_client, cluster, tasks)


