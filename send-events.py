import asyncio
import json
import random
import time
import argparse
import os

from pathlib import Path

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.storage.blob import BlobClient

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    RunRealtimeReportRequest,
    MinuteRange
)

STORAGE_ACCOUNT_CONNECTION_STRING = os.getenv("STORAGE_ACCOUNT_CONNECTION_STRING")
STORAGE_CONTAINER_NAME =  os.getenv("STORAGE_CONTAINER_NAME")
EVENT_HUB_NAME =  os.getenv("EVENT_HUB_NAME")
EVENT_HUB_CONNECTION_STR =  os.getenv("EVENT_HUB_CONNECTION_STR")
G4A_PROPERTY_ID =  os.getenv("G4A_PROPERTY_ID")

parser = argparse.ArgumentParser(
    prog='Sample Google Analytics Tickler'
)
parser.add_argument('-c', '--confignumber', type=int, default=0, dest='whichConfig')
args = parser.parse_args()

with open('./config.json') as c:
    config_json= c.read()

config = json.loads(config_json)

client = BetaAnalyticsDataClient()

random.seed()

categories = ["event","alert"]

async def run():
    print("Initializing producer...")

    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )
   
    async with producer:
        try:
            while True:
                messages = []
                event_data_batch = await producer.create_batch()
                timestamp = str(int(time.time()))

                if config[args.whichConfig]['type'] == 'report':
                    request = RunReportRequest(
                        property=f"properties/{G4A_PROPERTY_ID}",
                        date_ranges=[DateRange(start_date="yesterday", end_date="today")],
                    )
                    for d in config[args.whichConfig]['dimensions']:
                        request.dimensions.append(Dimension(name=d['dimension']))                
                    for m in config[args.whichConfig]['metrics']:
                        request.metrics.append(Metric(name=m['metric']))                

                    response = client.run_report(request)    
                else:
                    request = RunRealtimeReportRequest(
                        property=f"properties/{G4A_PROPERTY_ID}",
                        minute_ranges=[
                            MinuteRange(name="0-4 minutes ago", start_minutes_ago=4),
                        ],
                    )
                    for d in config[args.whichConfig]['dimensions']:
                        print(d['dimension'])
                        request.dimensions.append(Dimension(name=d['dimension']))                
                    for m in config[args.whichConfig]['metrics']:
                        print(m['metric'])
                        request.metrics.append(Metric(name=m['metric']))                

                    response = client.run_realtime_report(request)

                message = {
                    "messageType": args.whichConfig,
                    "data" : {}
                }
                json_data = []

                for row in response.rows:
                    data = {
                        str(config[args.whichConfig]['dimensions'][0]['dimension']):row.dimension_values[0].value,
                        str(config[args.whichConfig]['metrics'][0]['metric']):row.metric_values[0].value,
                        "asOf":timestamp
                    }
                    message["data"] = data
                    event_data_batch.add(EventData(json.dumps(message)))
                    messages.append(message)    
                           

                print('Sending batch...')
                await producer.send_batch(event_data_batch)

                blob_service = BlobClient.from_connection_string(conn_str=STORAGE_ACCOUNT_CONNECTION_STRING, container_name=STORAGE_CONTAINER_NAME, blob_name="{0}/{1}.json".format(args.whichConfig, timestamp))
                Path("./messages/{0}".format(args.whichConfig)).mkdir(parents=True, exist_ok=True)
                print("Saving to file {0}/{1}.json...".format(args.whichConfig,timestamp))
                with open("./messages/{0}/{1}.json".format(args.whichConfig,timestamp), "w") as f1:
                    f1.write(json.dumps(messages))
                print("Uploading blob {0}/{1}.json...".format(args.whichConfig,timestamp))                    
                with open("./messages/{0}/{1}.json".format(args.whichConfig,timestamp), "rb") as f2:
                    blob_service.upload_blob(f2)
                    print("Uploaded!")
                
                time.sleep(10)

        except KeyboardInterrupt:
            print("Stopping application...")
            pass

asyncio.run(run())