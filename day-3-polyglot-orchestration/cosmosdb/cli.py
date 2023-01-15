# import libraries
import os
import warnings
from dotenv import load_dotenv

from datastore.cosmosdb.sql import CosmosDB


# warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# get env
load_dotenv()

# load variables
get_dt_rows = os.getenv("EVENTS")


# main
if __name__ == '__main__':
    CosmosDB().insert_documents_sql_api()

