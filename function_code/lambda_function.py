import boto3
import re
import json
import sys
import csv
import os
from datetime import datetime
from io import StringIO


def move_file_to_final_bucket(stage_bucket, final_bucket, file_name, to_update, s3_client):
    file_prefix = file_name.replace(".csv", "")
    timestamp = datetime.now()
    if(to_update):
        destination_file_key = file_name
    else:
        destination_file_key = f"{file_prefix}/{timestamp.strftime('%Y/%m/%d/%H/%M')}/{file_name}"

    copy_source = {'Bucket': stage_bucket, 'Key': file_name}

    try:
        s3_client.copy_object(CopySource=copy_source,
                              Bucket=final_bucket, Key=destination_file_key)
        s3_client.delete_object(Bucket=stage_bucket, Key=file_name)
    except Exception as e:
        print(e)
        raise


def update_record(dynamodb_client, table_name, primary_key_values, rest_of_the_columns_values):
    dynamodb_client.update_item(
        TableName=table_name,
        Key={
            'PrimaryKeyColumns': {
                'S': ';'.join(primary_key_values),
            }
        },
        AttributeUpdates={
            'RestOfTheColumns': {
                'Value': {
                    'S': ';'.join(rest_of_the_columns_values)
                }
            }
        }
    )


def create_put_request(primary_key_values, rest_of_the_columns_values):
    return {
        'PutRequest': {
            'Item': {
                'PrimaryKeyColumns': {
                    'S': ';'.join(primary_key_values),
                },
                'RestOfTheColumns': {
                    'S': ';'.join(rest_of_the_columns_values),
                }
            }
        }
    }


def process_file_records(table_name, file_name, config_file_object, stage_bucket, dynamodb_client, s3_client, new_table_created):
    try:
        get_object_response = s3_client.get_object(
            Bucket=stage_bucket, Key=file_name)

        file_content = StringIO(get_object_response["Body"].read().decode())
        header = file_content.readline().split(';')
        primary_key_columns = config_file_object['Files'][file_name]['PrimaryKey']
        primary_key_columns_indexes = [header.index(
            column) for column in primary_key_columns]
        rest_of_the_columns_indexes = set(
            range(len(header))) - set(primary_key_columns_indexes)

        put_requests = []
        for line in file_content:
            row_splitted = line.split(';')
            primary_key_values = [row_splitted[i]
                                  for i in primary_key_columns_indexes]
            rest_of_the_columns_values = [row_splitted[i]
                                          for i in rest_of_the_columns_indexes]
            if(new_table_created):
                put_requests.append(
                    create_put_request(
                        primary_key_values, rest_of_the_columns_values))
            else:
                update_record(dynamodb_client, table_name,
                              primary_key_values, rest_of_the_columns_values)

        if(new_table_created):
            waiter = dynamodb_client.get_waiter('table_exists')
            waiter.wait(TableName=table_name)
            dynamodb_client.batch_write_item(
                RequestItems={table_name: put_requests})

    except Exception as e:
        if(new_table_created):
            print(
                f"File '{file_name}' records could not be loaded into the {table_name} table. Error message:\n{e}")
        else:
            print(
                f"Table {table_name} could not be updated. Error message:\n{e}") 
        raise


def create_new_table(table_name, dynamodb_client):
    try:
        dynamodb_client.create_table(
            TableName=table_name,
            BillingMode='PAY_PER_REQUEST',
            AttributeDefinitions=[
                {
                    'AttributeName': 'PrimaryKeyColumns',
                    'AttributeType': 'S',
                }
            ],
            KeySchema=[
                {
                    'AttributeName': 'PrimaryKeyColumns',
                    'KeyType': 'HASH',
                }
            ]
        )
    except Exception as e:
        print(
            f"Table {table_name} could not be created. Error message:\n{e}")
        raise


def check_if_table_exists(table_name, dynamodb_client):
    try:
        list_tables_response = dynamodb_client.list_tables()
        return table_name in list_tables_response['TableNames']
    except Exception as e:
        print(f"DynamoDB tables could not be listed. Error message:\n{e}")
        raise


def get_config_file(config_bucket, config_file_name, s3_client):
    try:
        config_file_response = s3_client.get_object(
            Bucket=config_bucket, Key=config_file_name)
        config_dictionary = json.loads(
            config_file_response["Body"].read().decode())
        return config_dictionary
    except Exception as e:
        print((f"Config file {config_file_name} could not be pulled "
               f"from the {config_bucket} bucket. Error message:\n{e}"))
        raise


def lambda_handler(event, context, local_debug=False):
    try:
        if local_debug:
            os.environ['CONFIG_BUCKET'] = 'data-lake-config'
            os.environ['FINAL_BUCKET'] = 'data-lake-final'
            os.environ['CONFIG_FILE_NAME'] = 'files_to_update.json'
            boto3.setup_default_session()
            with open("example_event.json", 'r') as f:
                event = json.load(f)

        config_bucket = os.environ['CONFIG_BUCKET']
        config_file_name = os.environ['CONFIG_FILE_NAME']
        final_bucket = os.environ['FINAL_BUCKET']

        event_record = event["Records"][0]
        stage_bucket = event_record["s3"]["bucket"]["name"]
        file_name = event_record["s3"]["object"]["key"]
        if not file_name.lower().endswith('.csv'):
            print(f'{file_name} extension was incorrect - CSV expected.')
            return

        s3_client = boto3.client('s3')
        dynamodb_client = boto3.client('dynamodb')

        config_file_object = get_config_file(
            config_bucket, config_file_name, s3_client)
        if(file_name not in config_file_object["Files"]):
            move_file_to_final_bucket(
                stage_bucket, final_bucket, file_name, False, s3_client)
            return

        table_name = f"data_updater_{file_name.replace('.csv', '')}"

        if(not check_if_table_exists(table_name, dynamodb_client)):
            create_new_table(table_name, dynamodb_client)
            process_file_records(table_name, file_name, config_file_object,
                                 stage_bucket, dynamodb_client, s3_client, True)
            move_file_to_final_bucket(
                stage_bucket, final_bucket, file_name, True, s3_client)
        else:
            process_file_records(table_name, file_name, config_file_object,
                                 stage_bucket, dynamodb_client, s3_client, False)
            

    except Exception as e:
        print(f'Data updater error. Error message {e}')


# lambda_handler("event", "context", True)
