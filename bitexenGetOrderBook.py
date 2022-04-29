import pymongo
import json
import requests
import time
import os
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler

#tried to use pandas for data analysis from mongodb but failed
#so it's unused at the moment.
from pandas import json_normalize

#train_model is for job scheduler.
def train_model():

    #creating a client to reach my MongoDB localhost.
    client = pymongo.MongoClient("localhost", 27017)

    #creating a database.
    db = client.test_database

    #created this function to use later in reaching
    #specific documents and its necessary fields but failed.
    Collection = db.Collection

    #To reach the specified url data.
    r = requests.get('https://www.bitexen.com/api/v1/order_book/BTCTRY/')

    #To convert data type and read it.
    #tried other methods such as loads() here as well
    #but those created some exceptions so found out
    #dumps() is the better way for this.
    r = r.json()
    data = json.dumps(r, indent=4)

    #tried to use pandas to create a tabular dataframe for the data
    #I wanted to take out of database but after so many fails
    #I decided to leave it like that. So now it shows in the console
    #the fields and some of details.
    df = pd.read_json(data)
    print(df)

    #used this to see if I print the respective data every 5 seconds
    #because I had some hard time to get the new data
    #print(data)

    #I tried so many different methods to reach into the data to extract information
    #so that I can create necessary stats but failed to do so. This was
    #the latest iteration of that experiment.
    """x = Collection.find({"market_code": "BTCTRY"}, {"buyers.orders_total_amount"})
    for data in x:
        print(data.get('buyers')[0].get('orders_total_amount'))"""

    ###To create a collection and insert the correct data type into it
    db.Collection.insert_one(r)

#founds some other ways to handle job scheduling but I got this one working
#it runs the code every 5 seconds. Used while loop with time.sleep() function at first
#but of course this is the right and more elegant way.
if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(train_model, 'interval', seconds=5)
    scheduler.start()
    print('Press Ctrl+(F2) to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()