# import libraries
import os
from dotenv import load_dotenv
from object.musics import Musics
from eventhubs_json import EventHubs

# get env
load_dotenv()
# load variables
get_dt_rows = os.getenv("SIZE")
# init variables
musics_object_name = Musics().get_multiple_rows(get_dt_rows)
# real-time data ingestion
EventHubs().send_music_data()
