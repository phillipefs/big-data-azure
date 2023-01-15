

# import libs
import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.exceptions import EventHubError
import time

# get env
load_dotenv()
# load variables

# variables connection with blob
connect_str = os.getenv("AZURE_CONN_BLOB")
container_landing = os.getenv("LANDING")
eh_name = os.getenv("EH_NAME_FILE_EVENTS")
eh_conn_string = os.getenv("EH_CONN_STRING")

# build azure blob storage
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_landing)

# list files from blob storage
try:
    print("\nListing blobs...")
    # List the blobs in the container
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        print("\t" + blob.name)
    @staticmethod
    def get_file_data(producer, file_list):
       # retrieve events in pandas format
       # send streams of data from a list
       event_data_list = file_list

    # event hub inserts
       try:
          # send batch
          producer = EventHubProducerClient.from_connection_string(conn_str=eh_conn_string, eventhub_name=eh_name)
          producer.send_batch(event_data_list)
       except ValueError:  # size exceeds limit.
          print("size of the event data list exceeds the size limit of a single send")
       except EventHubError as eh_err:
          print("sending error: ", eh_err)

          # send using producer class
          with producer:
             # call send events
             get_file_data(producer)

    # print exception from list blobs
except Exception as ex:
    print('Exception:')
    print(ex)
