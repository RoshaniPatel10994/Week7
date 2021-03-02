import logging
import time
import boto3
from botocore.exceptions import ClientError
ENDPOINT='http://localhost:8000'

def create_table():

    try:
        db_client = boto3.client('dynamodb', endpoint_url=ENDPOINT)
        db_client.create_table(
            AttributeDefinitions=[
            {
                  'AttributeName': 'pk',
                  'AttributeType': 'S',
              },
              {
                  'AttributeName': 'sk',
                  'AttributeType': 'S',
              },
              {
                  'AttributeName': 'price',
                  'AttributeType': 'N',
              }
          ],
          KeySchema=[
              {
                  'AttributeName': 'pk',
                  'KeyType': 'HASH',
              },
              {
                  'AttributeName': 'sk',
                  'KeyType': 'RANGE',
              }
          ],
          ProvisionedThroughput={
              'ReadCapacityUnits': 2,
              'WriteCapacityUnits': 2,
          },
          GlobalSecondaryIndexes=[
          {
                'IndexName': INDEX_NAME_DT,
                'KeySchema': [
                    {
                        'AttributeName': 'sk',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'pk',
                        'KeyType': 'RANGE'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 2,
                    'WriteCapacityUnits': 2
                }
            },
            {
                'IndexName': INDEX_NAME_P,
                'KeySchema': [
                    {
                      'AttributeName': 'sk',
                      'KeyType': 'HASH'
                    },
                    {
                      'AttributeName': 'price',
                      'KeyType': 'RANGE'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 2,
                    'WriteCapacityUnits': 2
                }
            },
          ],
          TableName=TABLE_NAME,
        )

        return True
    except ClientError as e:
        logging.error(e)
        return False
###############################################################################
# Describe a DynamoDb Table.
###############################################################################
def describe_table():

    try:
        db_client = boto3.client('dynamodb', endpoint_url=ENDPOINT)
        response = db_client.describe_table(TableName=TABLE_NAME)
        return response['Table']['TableStatus']
    except ClientError as e:
        logging.error(e)
        return None
###############################################################################
# Delete a DynamoDb Table.
###############################################################################
def delete_table():

    try:
        db_client = boto3.client('dynamodb', endpoint_url=ENDPOINT)
        db_client.delete_table(TableName=TABLE_NAME)
        return True
    except ClientError as e:
        logging.error(e)
        return False
###############################################################################
# Put a DynamoDb Item.
###############################################################################
def put_item(pk,sk,vendor,title,description,PrimaryUnits,PrimaryUnitsPerBottle,DoseUnit,DosePerPrimaryUnit,PriceUnits,price,CountrySold):

    try:
      db_client = boto3.client('dynamodb', endpoint_url=ENDPOINT)
      db_client.put_item(
        Item={
          'pk': {
            'S': pk,
          },
          'sk': {
            'S': sk,
          },
          'vendor': {
            'S': vendor,
          },
          'title': {
            'S': title,
          },
          'description': {
            'S': description,
          },
          'price': {
            'N': str(price), #Note: Even though a number, it is passed as a string
          },
          'PrimaryUnits':{
            'S': PrimaryUnits,
          },
          'PrimaryUnitsPerBottle':{
            'S': PrimaryUnitsPerBottle
          },
          'DoseUnit':{
            'S': DoseUnit
          },
          'DosePerPrimaryUnit':{
            'S': DosePerPrimaryUnit
          },
          'Price Units':{
            'S': PriceUnits
          },
          'Price':{
            'S':price
          },
          'Country Sold in':{
            'S':CountrySold 
          },

        },
        ReturnConsumedCapacity='TOTAL',
        TableName=TABLE_NAME,
      )
      return True
    except Exception as e:
        logging.error(e)
        return False
###############################################################################
# Get a DynamoDb Item.
###############################################################################
def get_item(pk,sk):

    try:
      db_client = boto3.client('dynamodb', endpoint_url=ENDPOINT)
      response = db_client.get_item(
        Key={
          'pk': {
            'S': pk,
          },
          'sk': {
              'S': sk,
          },      
        },
        TableName=TABLE_NAME,
      )
      print(response["Item"])
      return response["Item"]
    except ClientError as e:
        logging.error(e)
        return None
###############################################################################
###############################################################################
# Delete a DynamoDb Item.
###############################################################################
def delete_item(pk,sk):

    try:
      db_client = boto3.client('dynamodb', endpoint_url=ENDPOINT)
      response = db_client.delete_item(
        Key={
          'pk': {
            'S': pk,
          },
          'sk': {
              'S': sk,
          },      
        },
        TableName=TABLE_NAME,
      )
      print(response)
      return response
    except ClientError as e:
        logging.error(e)
        return None
###############################################################################
# Query a DynamoDb table.
# https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditionExpression
###############################################################################
def query():

    try:
      db_client = boto3.client('dynamodb', endpoint_url=ENDPOINT)
      response = db_client.query(
          ExpressionAttributeValues={
              ':v1': {
                  'S': 'b',
              },
              ':v2': {
                  'N': '1',
              },
              ':v3': {
                  'N': '20',
              }
          },
          #KeyConditionExpression='sk = :v2 AND pk = :v1',
          KeyConditionExpression='sk = :v1 AND price BETWEEN :v2 AND :v3',
          #KeyConditionExpression='sk = :v2 AND begins_with(pk,:v1)',
          ProjectionExpression='title, vendor, price',
          TableName = TABLE_NAME,
          IndexName = INDEX_NAME_P
      )
      return response["Items"]
    except Exception as e:
        logging.error(e)
        return None  
###############################################################################
# Query a DynamoDb table.
# https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditionExpression
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
###############################################################################
def scan():

    try:
      db_client = boto3.client('dynamodb', endpoint_url=ENDPOINT)
      response = db_client.scan(
          ExpressionAttributeNames={
              '#p': 'price',
              '#v': 'vendor',
              '#t': 'title'
          },
          ExpressionAttributeValues={
              ':v1': {
                  'S': 'dt',
              },
              # ':v2': {
              #     'N': '1',
              # },
              # ':v3': {
              #     'N': '10',
              # }
          },
          #FilterExpression='pk = :v1',
          FilterExpression='begins_with(pk,:v1)',
          #FilterExpression='#p BETWEEN :v2 AND :v3',
          ProjectionExpression='#p, #v, #t',
          TableName=TABLE_NAME,
      )
      return response['Items']
    except ClientError as e:
        logging.error(e)
        return None   
###############################################################################
# Exercise the DynamoDb functions.
###############################################################################
def main():
  create_table()

  while True:
    results = describe_table()
    if results == 'ACTIVE':
      break
    print("Please wait while your table is being created")
    time.sleep(10)

  #Add 3 items...
  put_item('vb',"1","Now Foods","Now Foods Thiamine B1 Energy Support","Vitamin B1, Called Thiamine, C12H17N4OS+","Pills","100","mg","100","USD","9.99","Worldwide")
  put_item('vb',"2","Now Foods","Now Foods Riboflavin B2 Energy Support","Vitamin B2, Called Riboflavin. C17H20N4O6","Pills","100","mg","100","USD","9.99","Worldwide")
  put_item('vb',"3","Now Foods","Now Foods Niacinamide B2 Energy Support","Vitamin B3, Called Niacinamide. C6H6N2O","Pills","100","mg","100","USD","9.99","Worldwide")

  # Add 3 more items...
  put_item('vb5',"b","Now Foods","Now Foods Thiamine B5 Energy Support","Vitamin B5, Called Pantothenic Acid. C8H11NO3","Pills","100","mg","100","USD","13","Worldwide")
  put_item('vb6',"b","Now Foods","Now Foods Pyridoxine B6 Energy Support","Vitamin B6, Called Pyridoxine. C8H11NO3","Pills","100","mg","100","USD","14","Worldwide")
  put_item('vb7',"b","Now Foods","Now Foods Niacinamide B7 Energy Support","Vitamin B7, Called Biotin. C10H16N2O3S","Pills","100","mg","100","USD","15","Worldwide")

  pk=input("Enter the pk of the item you would like to get: ")
  sk=input("Enter the sk of the item you would like to get: ")
  get_item(pk,sk)

  pk=input("Enter the pk of the item you would like to delete: ")
  sk=input("Enter the sk of the item you would like to delete: ")
  delete_item(pk,sk)

  print("Running sample Query now...")
  items = query()
  for item in items:
    print(item)


###############################################################################
# Exercise the DynamoDb functions.
###############################################################################
TABLE_NAME = input("What is the table name: ")
INDEX_NAME_DT = "VitaminName"
INDEX_NAME_P = "VitaminPrice"

main()
TABLE_NAME = input("What is the name of the table you would like to delete: ")
delete_table()

