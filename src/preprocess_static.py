#!/usr/bin/env python
# coding: utf-8

# # Preprocessing
# 1. convenience functions for date processing
# 2. fetch second latest static gtfs zips
# 4. select relevant routes, trips and stop_times
# 5. add start and end times for trips
# 6. save filtered data to filesystem

# Download latest rnv-gtfs data, unzip and read the data from the files:

# ## 1. convenience functions for gtfs date formats

# In[25]:


import datetime

def parseGtfsTimestringAsTimeObject(timestring:str):
    # mod 24, because gtfs defines days as service days that can be longer than 24 hours, so 24:15 is a valid gtfs time
    hour = int(timestring[0:2]) % 24
    minute = int(timestring[3:5])
    second = int(timestring[6:8])
    #print(timestring)
    #print(hour)
    #print(minute) 
    #print(second)
    return datetime.time(hour, minute, second)

def parseGtfsDatestringAsDateObject(datestring:str):
    year = int(datestring[0:4])
    month = int(datestring[4:6])
    day = int(datestring[6:8])
    return datetime.date(year, month, day)

def addSecondsToTimeObject(time:datetime.time, seconds) -> datetime.time:
    datetime_object = datetime.datetime(100,1,1,time.hour, time.minute, time.second)
    delta = datetime.timedelta(seconds=seconds)
    return (datetime_object + delta).time()


# # 2. fetch second latest gtfs zip
# 
# rnv publishes static gtfs on thursdays, with data valid from the following monday for 1-2 weeks. Therefore, we will always use the gtfs published in the previous week, to prevent switching to the latest one too early. 
# 
# convenience function for downloading and extracting zip:

# In[26]:


# convenience function for downloading and extracting zip
def download_and_extract_zip(url, extract_to='.'):
    # download file
    response = requests.get(url)
    response.raise_for_status()  

    # extract in memory
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
        # extract to disk
        zip_ref.extractall(extract_to)
        print(f"extracted to '{extract_to}'")


# fetch and extract the gtfs zip:

# In[27]:


from pandas import read_csv, DataFrame
from os import path, getcwd, getenv
from dotenv import load_dotenv
import requests, zipfile, io
import json

load_dotenv()
# get array of definitions for the gtfs versions

gtfs_base_url = getenv('gtfs_base_url')
gtfs_version_overview_url = gtfs_base_url

response = requests.get(gtfs_version_overview_url)

gtfs_versions_dict = json.loads(response.text)

# TODO pick correct gtfs by taking the one from last week

gtfs_url = ''

# search for gtfs of last week, once found, build gtfs_url
today = datetime.date.today()
days_since_last_sunday = today.weekday() + 1

lastWeekEnd = today - datetime.timedelta(days = days_since_last_sunday)
lastWeekStart = lastWeekEnd - datetime.timedelta(days = 6)

# newest versions are at the end
for i, gtfs_version in enumerate(gtfs_versions_dict[-4:]):
    modifiedAt = datetime.datetime.fromtimestamp(gtfs_version['modified'] / 1000, datetime.UTC).date()  
    print(modifiedAt)
    # error in the api
    # an old gtfs package was reuploaded  
    if gtfs_version['modified'] == 1751033303000:
        continue
    if lastWeekStart <= modifiedAt <= lastWeekEnd:
        gtfs_url = f"{gtfs_base_url}/{gtfs_version['dir']}/gtfs.zip"


if gtfs_url == '':
    raise Exception()

print(gtfs_url)

# fetch data
download_and_extract_zip(gtfs_url, './gtfs_full')



# ## 3. filter relevant routes, trips and stop_times
# 
# Before we start, lets load the data from the filesystem.

# In[28]:


gtfs_path = path.join(getcwd(), 'gtfs_full')
calendar_path = path.join(gtfs_path, 'calendar.txt')
routes_path = path.join(gtfs_path, 'routes.txt')
trips_path = path.join(gtfs_path, 'trips.txt')
stops_path = path.join(gtfs_path, 'stops.txt')
stop_times_path = path.join(gtfs_path, 'stop_times.txt')

calendar:DataFrame = read_csv(calendar_path)
routes:DataFrame = read_csv(routes_path)
trips:DataFrame = read_csv(trips_path)
stops:DataFrame = read_csv(stops_path)
stop_times:DataFrame = read_csv(stop_times_path)

print('read gtfs static data from files')


# First, we want to remove all unneccessary data entries.
# As we will focus on the line 22 for the start, we only want routes, trips and stop_times for the line 22. 

# In[29]:


relevant_lines = ['22', '26', '5', '23', '21', '24']
relevant_trip_prefixes = [line + "-" for line in relevant_lines]


# To achieve this, we firstly  select all rows from the routes that have a ´route_id´ starting with 22, indicating the route to be on line 22. By doing this instead of looking at the ´route_short_name´, special services like line E for shortened services to and from the depot are included.

# In[30]:


# select relevant columns
routes = routes[['route_id', 'route_short_name', 'route_desc', 'route_color']]

# select only routes of relevant lines, indicated by the route_id 
routes = routes.loc[routes['route_id'].str.startswith(tuple(relevant_trip_prefixes))]

print('found ',routes.shape[0], 'routes on lines', relevant_lines)
print(routes.head(5))


# Let's do the same with trips.

# In[31]:


# select relevant columns
trips = trips[["route_id","trip_id", "service_id", "trip_short_name"]]

# select only trips of relevant lines, indicated by the trip_id 
trips = trips.loc[trips['trip_id'].str.startswith(tuple(relevant_trip_prefixes))]

print('found ',trips.shape[0], 'trips on lines', relevant_lines)
print(trips.head(5))


# And finally, we also filter the stop_times by looking at the prefix of the trip_id.

# In[32]:


# select relevant columns
stop_times = stop_times[["trip_id", "arrival_time", "departure_time", "stop_sequence", "stop_id"]]

# select only stop_times of relevant lines, indicated by the trip_id 
stop_times = stop_times.loc[stop_times['trip_id'].str.startswith(tuple(relevant_trip_prefixes))]

print('found ',stop_times.shape[0], 'stop times on lines', relevant_lines)
print(stop_times.head(5))


# ## 4. (optional) adjust arrivals and departures for visualization
# The schedule only uses minutes and not seconds. This results in most stops having a standing time of 0 seconds. At the same time, there are no two stops that are scheduled to arrive in the same minute. Therefore, we can manually add an artificial departure delay of 15 seconds, which we will account for when dealing with real time delays later on.

# In[33]:


def addArtificialDepartureDelay(row):
    departure_time_object = parseGtfsTimestringAsTimeObject(row['departure_time'])
    adjusted_departure_time_object = addSecondsToTimeObject(departure_time_object, 15)
    row['departure_time'] = adjusted_departure_time_object.isoformat()
    return row

stop_times = stop_times.apply(addArtificialDepartureDelay, axis=1)
print(stop_times[:5])


# ## 5. add start and end times to trips

# To make it easy to identify the active trips, we will now add start and end times to each trip.
# First, we will create a function to get all the stop_times for a specific ´trip_id´. Then we will sort the stop_times and return the first ´arrival_time´ as trip start and the last ´departure_time´ as trip end.

# In[34]:


def getTripStartTime(trip_id:str) -> tuple[str, str]:
    relevant_stop_times = stop_times.loc[stop_times['trip_id'] == trip_id]
    #print('found ',relevant_stop_times.shape[0], 'relevant stop times for trip_id', trip_id)

    relevant_stop_times = relevant_stop_times.sort_values(by=['stop_sequence'])

    first_stop = relevant_stop_times.iloc[0]
    trip_start_time = first_stop.loc['arrival_time']

    return trip_start_time

def getTripEndTime(trip_id:str) -> tuple[str, str]:
    relevant_stop_times = stop_times.loc[stop_times['trip_id'] == trip_id]
    #print('found ',relevant_stop_times.shape[0], 'relevant stop times for trip_id', trip_id)

    relevant_stop_times = relevant_stop_times.sort_values(by=['stop_sequence'])

    last_stop = relevant_stop_times.iloc[-1]
    trip_end_time = last_stop.loc['departure_time']

    return trip_end_time

example_trip_id = trips.iloc[0].loc['trip_id']
example_start = getTripStartTime(example_trip_id)
example_end = getTripEndTime(example_trip_id)
print('trip_id:', example_trip_id, '\nTrip Start Time: ', example_start, '\nTrip End Time: ', example_end)


# Now let's add the new columns by using the function we just created.

# In[35]:


trips['start_time'] = trips.apply(lambda row: getTripStartTime(row['trip_id']), axis=1)
trips['end_time'] = trips.apply(lambda row: getTripEndTime(row['trip_id']), axis=1)

print(trips.head(5))


# ## 6. save filtered data to filesystem

# In[36]:


import os

gtfs_filtered_path = path.join(getcwd(), 'gtfs_filtered')

if not os.path.exists(gtfs_filtered_path):
   os.makedirs(gtfs_filtered_path)

calendar_filtered_path = path.join(gtfs_filtered_path, 'calendar.txt')
routes_filtered_path = path.join(gtfs_filtered_path, 'routes.txt')
trips_filtered_path = path.join(gtfs_filtered_path, 'trips.txt')
stops_filtered_path = path.join(gtfs_filtered_path, 'stops.txt')
stop_times_filtered_path = path.join(gtfs_filtered_path, 'stop_times.txt')


calendar.to_csv(calendar_filtered_path, index=False)
routes.to_csv(routes_filtered_path, index=False)
trips.to_csv(trips_filtered_path, index=False)
stops.to_csv(stops_filtered_path, index=False)
stop_times.to_csv(stop_times_filtered_path, index=False)


# In[ ]:




