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


def create_updated_csv_file(header, table_name, file_name, final_bucket, s3_client):
    dynamodb_resource = boto3.resource('dynamodb')
    table = dynamodb_resource.Table(table_name)
    scan_kwargs = {
        'ProjectionExpression': ','.join(header)
    }

    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        items = response.get('Items', [])
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    final_rows = [';'.join(header)]
    for item in items:
        final_row = []
        for column in header:
            if column in item:
                final_row.append(item[column])
            else:
                final_row.append('')
        final_rows.append(';'.join(final_row))

    final_file_binary = ('\n'.join(final_rows)).encode('utf8')

    try:
        s3_client.put_object(
            Body=final_file_binary, Bucket=final_bucket, Key=file_name)
    except Exception as e:
        print(
            f'Updated file could not be put to the {final_bucket} bucket. Error message {e}')
        raise


def update_record(dynamodb_client, table_name, row_splitted, header, primary_key_index):

    column_values_dict = dict()
    for i in range(len(header)):
        if(i != primary_key_index):
            column_values_dict[header[i]] = {
                'Value': {
                    'S': row_splitted[i]
                }
            }

    dynamodb_client.update_item(
        TableName=table_name,
        Key={
            header[primary_key_index]: {
                'S': row_splitted[primary_key_index],
            }
        },
        AttributeUpdates=column_values_dict
    )


def create_put_request(row_splitted, header):

    column_values_dict = dict()
    for i in range(len(header)):
        column_values_dict[header[i]] = {
            'S': row_splitted[i]
        }

    return {
        'PutRequest': {
            'Item': column_values_dict
        }
    }


def create_new_table(table_name, dynamodb_client, primary_key_column):
    try:
        dynamodb_client.create_table(
            TableName=table_name,
            BillingMode='PAY_PER_REQUEST',
            AttributeDefinitions=[
                {
                    'AttributeName': primary_key_column,
                    'AttributeType': 'S',
                }
            ],
            KeySchema=[
                {
                    'AttributeName': primary_key_column,
                    'KeyType': 'HASH',
                }
            ]
        )
    except Exception as e:
        print(
            f"Table {table_name} could not be created. Error message:\n{e}")
        raise


def process_file_records(table_name, file_name, config_file_object, final_bucket, stage_bucket, dynamodb_client, s3_client, table_exists):
    try:
        get_object_response = s3_client.get_object(
            Bucket=stage_bucket, Key=file_name)

        file_content = StringIO(get_object_response["Body"].read().decode())
        header = file_content.readline().replace('\n', '').split(';')
        primary_key = config_file_object['Files'][file_name]['PrimaryKey']
        primary_key_index = header.index(primary_key)

        if(not table_exists):
            create_new_table(table_name, dynamodb_client, primary_key)

        put_requests = []
        for line in file_content:
            row_splitted = line.replace('\n', '').split(';')
            if(not table_exists):
                put_requests.append(
                    create_put_request(
                        row_splitted, header))
            else:
                update_record(dynamodb_client, table_name,
                              row_splitted, header, primary_key_index)

        if(not table_exists):
            waiter = dynamodb_client.get_waiter('table_exists')
            waiter.wait(TableName=table_name)
            dynamodb_client.batch_write_item(
                RequestItems={table_name: put_requests})
            move_file_to_final_bucket(
                stage_bucket, final_bucket, file_name, True, s3_client)
        else:
            create_updated_csv_file(
                header, table_name, file_name, final_bucket, s3_client)

    except Exception as e:
        if(not table_exists):
            print(
                f"File '{file_name}' records could not be loaded into the {table_name} table. Error message:\n{e}")
        else:
            print(
                f"Table {table_name} could not be updated. Error message:\n{e}")
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

        table_exists = check_if_table_exists(table_name, dynamodb_client)

        process_file_records(table_name, file_name, config_file_object, final_bucket,
                             stage_bucket, dynamodb_client, s3_client, table_exists)

    except Exception as e:
        print(f'Data updater error. Error message {e}')


# lambda_handler("event", "context", True)
