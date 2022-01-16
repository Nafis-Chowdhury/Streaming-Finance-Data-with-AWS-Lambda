import json
import boto3
import yfinance as yf


startDate = '2021-11-30'
endDate = '2021-12-01'
everyInterval = '5m'   
tickers = ['FB', 'SHOP', 'BYND', 'NFLX', 'PINS', 'SQ', 'TTD', 'OKTA', 'SNAP', 'DDOG']


kinesis = boto3.client('kinesis', "us-west-1")

def lambda_handler(event, context):
    for ticker in tickers:
        data = yf.download(ticker, start=startDate, end=endDate, interval = everyInterval)
        for Datetime, row in data.iterrows():
            record = {
              'high': row['High'],
              'low': row['Low'],
              'ts': str(Datetime), 
              'name': ticker
              }
            
            recordJSON = json.dumps(record)+"\n"
            
            kinesis.put_record(
                StreamName="project3_data_stream",
                Data=recordJSON,
                PartitionKey="partitionkey"
                )
    return {
        'statusCode': 200,
        'body': json.dumps("Hello from Lambda!")
    }