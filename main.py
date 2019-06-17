from pymongo import Connection
import pymongo
import datetime
connection = Connection()
connection = Connection('localhost', 27017)
db = connection.switch_on
collection = db.production
production_records = collection.find()
result = list()
db.hourly_performance.drop()
for prod in production_records:
   start_time = prod.get('published_at') 
   end_time = prod.get('published_at')  + datetime.timedelta(minutes = 60)

   query_result = collection.find({
         "$and": [{"published_at" : {"$lt": end_time}},
                  {"published_at" : {"$gte": datetime.datetime(2019, 5,14 )}},
                  {"published_at" : {"$lte": datetime.datetime(2019, 5,15 )}}
                 ]
                }).sort('published_at',pymongo.DESCENDING)
   if(query_result):
    hourly_record = list(query_result)[0] # get the record which comes last after adding 60 minutes
    production = hourly_record.get('value') - prod.get('value') # get the production value
    run_time = 60 # in minutes
    max_ppm = 40  # parts per minute
    performance = ( production / ( run_time* max_ppm ) ) * 100 
    result.append({"start_time": start_time, "performance": performance })

db.hourly_performance.insert(result)