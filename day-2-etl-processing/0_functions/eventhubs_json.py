# import libraries
import os
import time
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.exceptions import EventHubError

# get env
load_dotenv()

# load variables
eh_name = os.getenv("EH_NAME_MUSIC_EVENTS")
eh_conn_string = os.getenv("EH_CONN_STRING")
size = os.getenv("SIZE")

# class to insert into datastore
class EventHubs(object):

    @staticmethod
    def get_file_data(producer, file_list):
        # retrieve events in pandas format
        # send streams of data from a list
        event_data_list = file_list

        # print events
        # 100 events per time
        # print(event_data_list)

        # try to send events
        try:
            # send batch
            producer.send_batch(event_data_list)
        except ValueError:  # size exceeds limit.
            print("size of the event data list exceeds the size limit of a single send")
        except EventHubError as eh_err:
            print("sending error: ", eh_err)

    def send_music_data(self):
        # setting up connectivity to azure event hubs
        producer = EventHubProducerClient.from_connection_string(conn_str=eh_conn_string, eventhub_name=eh_name)

        # get time to send event (event time)
        start_time = time.time()

        # send using producer class
        with producer:
            # call send events
            self.get_file_data(producer)

        # events written successfully on azure event hubs [time taken to send]
        print("amount of events sent {}.".format(size))
        print("send messages in {} seconds.".format(round(time.time() - start_time, 2)))