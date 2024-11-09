#!/usr/bin/env python
# coding: utf-8

# # Extract active vehicles
# 1. Convenience functions for date processing
# 2. fetch trip_updates
# 3. Process trip_updates
# 4. Enrich stop_times with real-time trip_updates
# 5. add realtime start and end times to trips
# 6. Get trips that are currently active
# 7. Get status of the active trips
# 8. Transform status to LED matrix

# In[99]:


import pandas as pd
from os import path, getcwd

gtfs_filtered_path = path.join(getcwd(), 'gtfs_filtered')
calendar_path = path.join(gtfs_filtered_path, 'calendar.txt')
routes_path = path.join(gtfs_filtered_path, 'routes.txt')
trips_path = path.join(gtfs_filtered_path, 'trips.txt')
stops_path = path.join(gtfs_filtered_path, 'stops.txt')
stop_times_path = path.join(gtfs_filtered_path, 'stop_times.txt')

calendar:pd.DataFrame = pd.read_csv(calendar_path)
routes:pd.DataFrame = pd.read_csv(routes_path)
trips:pd.DataFrame = pd.read_csv(trips_path)
stops:pd.DataFrame = pd.read_csv(stops_path)
stop_times:pd.DataFrame = pd.read_csv(stop_times_path)


relevant_lines = ['22', '5', '26', '23', '21']
#relevant_lines = ['23']
relevant_trip_prefixes = [line + "-" for line in relevant_lines]


# ## 1. convenience functions for gtfs date formats

# In[100]:


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
    datestring = str(datestring)
    year = int(datestring[0:4])
    month = int(datestring[4:6])
    day = int(datestring[6:8])
    return datetime.date(year, month, day)

def addSecondsToTimeObject(time:datetime.time, seconds) -> datetime.time:
    datetime_object = datetime.datetime(100,1,1,time.hour, time.minute, time.second)
    delta = datetime.timedelta(seconds=seconds)
    return (datetime_object + delta).time()


def getGtfsWeekdayFromDate(date: datetime.date):
    weekday_number = date.weekday()
    if weekday_number == 0:
        return "monday"
    elif weekday_number == 1:
        return "tuesday"
    elif weekday_number == 2:
        return "wednesday"
    elif weekday_number == 3:
        return "thursday"
    elif weekday_number == 4:
        return "friday"
    elif weekday_number == 5:
        return "saturday"
    else:
        return "sunday"


# ## 2. Fetch trip_updates
# 
# Now we want to fetch the trip_updates from the realtime api to later enrich our static schedules with real time delay data.
# To do that, we must first authenticate via oauth2 and then call the tripupdates endpoint.

# In[101]:


# load env
from dotenv import load_dotenv
from os import getenv
import requests

load_dotenv()

# authenticate with oauth2
client_id = getenv('gtfs_rt_clientID')
client_secret = getenv('gtfs_rt_clientSecret')
resource = getenv('gtfs_rt_resource')
tenant_id = getenv('gtfs_rt_tenantID')
hostname = getenv('gtfs_rt_hostname')


from oauthlib.oauth2 import WebApplicationClient
client = WebApplicationClient(client_id)

# prepare x-www-form-urlencoded body data
data = f'grant_type=client_credentials&resource={resource}&client_id={client_id}&client_secret={client_secret}'

auth_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'
headers= { 'Content-type':'application/x-www-form-urlencoded'}

auth_response = requests.post(auth_url, data=data, headers=headers)
auth = client.parse_request_body_response(auth_response.text)

gtfs_access_token = auth['access_token']

# fetch tripupdates
import json

trip_updates_json_url = f'{hostname}/tripupdates/decoded'
headers = {'Authorization':f'Bearer {gtfs_access_token}'}

trip_updates_response = requests.get(trip_updates_json_url, headers=headers)
trip_updates = json.loads(trip_updates_response.text)['entity']

trip_updates = [trip_update['tripUpdate'] for trip_update in trip_updates]

print(trip_updates[0])
#print(trip_updates['entity'][0]['tripUpdate']['stopTimeUpdate'])







# ## 3. preprocess data
# 
# Firstly, we need to select only trip_updates, trips, stop_times, stops and routes for our relevant lines to reduce unnecessary processing.
# Furhtermore, we only want trips and stop_times that run + - 1 hour of the current time, assuming that no train has more than 60 minutes of delay, to reduce unnecessary processing.

# In[102]:


# select only trip_updates of relevant trips, indicated by the refernced trip.tripId
trip_updates = [trip_update for trip_update in trip_updates if trip_update['trip']['tripId'].startswith(tuple(relevant_trip_prefixes))]

# select only routes, trips and stop_times of relevant lines, indicated by the route_id / trip_id
routes = routes.loc[routes['route_id'].str.startswith(tuple(relevant_trip_prefixes))]
trips = trips.loc[trips['trip_id'].str.startswith(tuple(relevant_trip_prefixes))]
stop_times = stop_times.loc[stop_times['trip_id'].str.startswith(tuple(relevant_trip_prefixes))]

current_datetime = datetime.datetime.now()

# train is potentially running if
# 1. the scheduled start is before the current time (otherwise trip hasn't started yet)
# 2. the current time if before the scheduled end + 2 hours (otherwise trip has ended, unless delay is > 2h)
def isPotentiallyRunningAtCurrentTime(start_gtfs_timestring, end_gtfs_timestring, current_datetime):
    starttime = parseGtfsTimestringAsTimeObject(start_gtfs_timestring)
    endtime = parseGtfsTimestringAsTimeObject(end_gtfs_timestring)

    # make a datetime with the current date, because the selected trips are scheduled for today
    # if we use time instead of datetime, no trips after 00:00 - delay_buffer can be shown
    startdatetime = datetime.datetime.combine(datetime.date.today(), starttime)
    enddatetime = datetime.datetime.combine(datetime.date.today(), endtime)

    enddatetime_with_delay_buffer = enddatetime + datetime.timedelta(hours=2)

    return startdatetime <= current_datetime <= enddatetime_with_delay_buffer
    

# select only trips that are potentially running right now, ignoring trains with 2h + delay
trips = trips.loc[trips.apply(lambda row: isPotentiallyRunningAtCurrentTime(row['start_time'], row['end_time'], current_datetime), axis=1)]
stop_times = stop_times.loc[stop_times.apply(lambda row: row['trip_id'] in trips.loc[:,'trip_id'].values, axis=1)]

print(trips.head(5))
print(stop_times.head(5))


# According to gtfs-rt specification, the stopTimeUpdates only include updates of the delay. If a tram is delayed for 30 seconds departing stop 1, arriving at stop 2, departing stop 2 and then gets to stop 3 on time, the stopTimeUpdates will only include one entry for delay 30 (departure) at stop 1 and delay 0 (arrival) stop 3.
# To prepare enriching the stop_times with the delays, we simply fill the missing stopTimeUpdates.
# We will later use stopSequence to identify a stop, because we can simply calculate the stopSequence for the artificially filled stopTimeUpdated, but can't do it as easily with the stopIds.

# In[103]:


# iterate over the trip_updates
for trip_update in trip_updates:
    # find the last stopSequence for the trip
    trip_id = trip_update['trip']['tripId']
    schedule_relationship = trip_update['trip']['scheduleRelationship']
    
    # delete trip and stop_times for canceled trips
    if schedule_relationship == 'CANCELED':
        # only keep stop times / trips that are not related to the canceled trip
        stop_times = stop_times[stop_times['trip_id'] != trip_id]
        trips = trips[trips['trip_id'] != trip_id]
        print('deleting trip:', trip_id)
        continue

    stop_times_for_trip = stop_times.loc[stop_times['trip_id'] == trip_id]

    # skip updates for unknown trips, e.g. emergency services not known to GTFS schedule
    if len(stop_times_for_trip) == 0:
        continue
        
    #print(stop_times_for_trip)
    stop_times_for_trip = stop_times_for_trip.sort_values(by=['stop_sequence'])
    last_stop_sequence = int(stop_times_for_trip.iloc[-1]['stop_sequence']) 

    stop_time_updates = trip_update['stopTimeUpdate']
    stop_time_updates_filled = []

    # fill stop_time_updates for every stopSequence
    current_trip_delay_seconds = 0
    for stop_sequence in range(1,last_stop_sequence + 1):
        
        # check if stopTimeUpdate exists
        existing_stop_time_updates = [stop_time_update for stop_time_update in stop_time_updates if stop_time_update['stopSequence'] == stop_sequence]
        # no stopTimeUpdate exists, generate a new one with current_trip_delay
        if len(existing_stop_time_updates) == 0:
            stop_time_updates_filled.append({'stopSequence': stop_sequence, 
                                             'arrival': {'delay': current_trip_delay_seconds}, 
                                             'departure': {'delay': current_trip_delay_seconds}})
        # otherwise use the delays that already exist, update current_trip delay and fill arrival and departure with current_trip_delay if missing
        
        else:
            # determine arrival_delay
            existing_stop_time_update = existing_stop_time_updates[0]

               
            arrival_delay = existing_stop_time_update['arrival']['delay'] if 'arrival' in existing_stop_time_update else current_trip_delay_seconds
            
            # update current trip delay, if no arrival delay was specified, it virtually stays the same
            current_trip_delay_seconds = arrival_delay

            # determine departure_delay
            existing_stop_time_update = existing_stop_time_updates[0]
            departure_delay = existing_stop_time_update['departure']['delay'] if 'departure' in existing_stop_time_update else current_trip_delay_seconds
            
            # update current trip delay, if no arrival delay was specified, it virtually stays the same
            current_trip_delay_seconds = departure_delay

            stop_time_updates_filled.append({'stopSequence': stop_sequence, 
                                             'arrival': {'delay': arrival_delay}, 
                                             'departure': {'delay': departure_delay}})

    # replace stopTimeUpdate with filled version
    trip_update['stopTimeUpdate'] = stop_time_updates_filled

try:
    print(trip_updates[0])
except IndexError:
    print('no trip updates found')
    


# ## 4. enrich stop_times with realtime delays
# 
# 
# 
# Now, we can add the real time delay to the scheduled stop_times.
# We create two new columns, arrival_realtime and departure_realtime, and calculate the realtime arrival and departure times using the trip_updates from the previous step. If no trip_update exists, we will simply copy the scheduled times.

# In[104]:


def calculateRealtime(stop_time, arrival_or_departure):
    
    trip_id = stop_time['trip_id']
    scheduled_time = stop_time[f'{arrival_or_departure}_time']
    stop_sequence = stop_time['stop_sequence']
    
    # find the corresponding trip_update, if it exists
    trip_updates_for_stop_time = [trip_update for trip_update in trip_updates if trip_update['trip']['tripId'] == trip_id]
    
    # if no trip updates exist, the scheduled time is used instead
    if len(trip_updates_for_stop_time) == 0:
       return scheduled_time
   
    trip_update_for_stop_time = trip_updates_for_stop_time[0]
    
    # find the stopTimeUpdate for this stop
    stop_time_updates_for_stop_time = [stop_time_update for stop_time_update in trip_update_for_stop_time['stopTimeUpdate']]

    # if no stop time updates exist, the scheduled time is used instead
    if len(stop_time_updates_for_stop_time) == 0:
        return scheduled_time

    stop_time_update_for_stop_time = stop_time_updates_for_stop_time[0]

    
    # add delay to scheduled time
    scheduled_time_object = parseGtfsTimestringAsTimeObject(scheduled_time)
    delay = stop_time_update_for_stop_time[arrival_or_departure]['delay']
    # account for artificially added departure delay of 15 seconds from preprocessing 3.
    # => departure delays up to 15 seconds are already accounted for
    if arrival_or_departure == 'departure':
        delay = max(delay - 15,0)
        
    realtime = addSecondsToTimeObject(scheduled_time_object, delay).isoformat()

    return realtime
                                       


arrivals_realtime = [calculateRealtime(stop_time, 'arrival') for i, stop_time in stop_times.iterrows()]
departures_realtime = [calculateRealtime(stop_time, 'departure') for i, stop_time in stop_times.iterrows()]

# add columns to stop_times

stop_times['arrival_realtime'] = arrivals_realtime
stop_times['departure_realtime'] = departures_realtime


print(stop_times[:5])


# ## 5. add realtime start and end times to trips
# To make it easy to identify the active trips, we will now add start and end times to each trip. First, we will create a function to get all the stop_times for a specific `trip_id`. Then we will sort the stop_times and return the first `arrival_time` as trip start and the last `departure_time` as trip end.

# In[105]:


def getTripStartRealtime(trip_id:str) -> tuple[str, str]:
    relevant_stop_times = stop_times.loc[stop_times['trip_id'] == trip_id]
    #print('found ',relevant_stop_times.shape[0], 'relevant stop times for trip_id', trip_id)
    
    relevant_stop_times = relevant_stop_times.sort_values(by=['stop_sequence'])
    
    first_stop = relevant_stop_times.iloc[0]
    trip_start_time = first_stop.loc['arrival_realtime']

    return trip_start_time

def getTripEndRealtime(trip_id:str) -> tuple[str, str]:
    relevant_stop_times = stop_times.loc[stop_times['trip_id'] == trip_id]
    #print('found ',relevant_stop_times.shape[0], 'relevant stop times for trip_id', trip_id)
    
    relevant_stop_times = relevant_stop_times.sort_values(by=['stop_sequence'])
    
    last_stop = relevant_stop_times.iloc[-1]
    trip_end_time = last_stop.loc['departure_realtime']
    
    return trip_end_time


# Now let's add the new columns by using the function we just created.

# In[106]:


trips['start_realtime'] = trips.apply(lambda row: getTripStartRealtime(row['trip_id']), axis=1)
trips['end_realtime'] = trips.apply(lambda row: getTripEndRealtime(row['trip_id']), axis=1)

print(trips.head(5))


# ## 6. currently active trips

# First, we need to get all the trip_ids for currently active trips. Trips are active, if the current time is between the start and end time of the trip and if one of the services, the trip belongs to, runs on the current day.
# Let's start by looking at the start and end times of the trips.

# In[107]:


print(datetime.datetime.now())

def isTripRowActiveAtCurrentTime(trip_row):
    start_time = parseGtfsTimestringAsTimeObject(trip_row['start_realtime'])
    current_time = datetime.datetime.now().time() 
    end_time = parseGtfsTimestringAsTimeObject(trip_row['end_realtime'])
    #print(start_time, current_time, end_time, start_time <= current_time <= end_time)
    return start_time <= current_time <= end_time
    

# select trips where current time is between start and end time
trips = trips[trips.apply(isTripRowActiveAtCurrentTime, axis=1)]
print("found", trips.shape[0], "trips that run at the current time")
print(trips.head(5))


# Secondly, we will check whether the services run on the current day by looking up the services from the `service_id` column in the calendar dataframe.
# As soon as we find a `service_id` that runs on the current day, we can stop the search and return true, otherwise we return false.

# In[108]:


def isTripRowActiveOnCurrentDay(trip_row):
    current_date = datetime.date.today()
    current_weekday_gtfs = getGtfsWeekdayFromDate(datetime.date.today())
    
    calendar:pd.DataFrame = pd.read_csv(calendar_path)

    # select row from calendar for this service
    calendar = calendar[calendar['service_id'] == trip_row['service_id']]

    # check every calendar entry
    for index, schedule in calendar.iterrows():
        # check if current date is between start_date and end_date (inclusive)
        start_date = parseGtfsDatestringAsDateObject(schedule['start_date'])
        end_date = parseGtfsDatestringAsDateObject(schedule['end_date'])

        duration_check = start_date <= current_date <= end_date

        # check if current weekday is an active day in the schedule
        weekday_check = schedule[current_weekday_gtfs] == 1

        if duration_check and weekday_check:
            return True
                
    return False
    
trips = trips[trips.apply(isTripRowActiveOnCurrentDay, axis=1)]
print(trips.head(5))


# ## 7. Status of active trips
# Now that we have identified all the trips that are currently running, we want to know where the trams are on our network. As we later want to represent a vehicle being at a stop as well as a vehicle traveling between stops, we will represent the status of a vehicle (trip) as 
# 
# trip_id: <strip_id>, status: IN_TRANSIT_TO / STOPPED_AT, current_stop_id: <stop_id/None>, previous_stop_id: <stop_id>, next_stop_id: <stop_id>
# 
# This will be condensed into a status code string, which is then mapped to one or more LEDs, which should be lighted, when a vehicle has the respective status code.
# 
# Status codes for vehicles in transit will have the pattern previousstopid_nextstopid (2 stop ids separated by underscore), vehicles that have stopped at a station will have the pattern  previousstopid_currentstopid_nextstopid (3 stop ids separated by underscore).
# 

# First, let's define some functions:

# In[109]:


import pandas as pd

# create status Dataframe for every active trip, then merge the Dataframes
# status, current_stop_id, previous_stop_id


current_time = datetime.datetime.now().time()


def isStoppedAtStopTime(stop_time):
    return parseGtfsTimestringAsTimeObject(stop_time['arrival_realtime']) <= current_time <= parseGtfsTimestringAsTimeObject(stop_time['departure_realtime'])

# take stop times and iterator to check previous stop
# check if the stop_time at position i of stop_times is currently being traveled to
def isTravelingToStoptime(stop_times, i):
    # loc because i is the pandas index of the row 
    current_stop_time = stop_times.loc[i]

    # if there is no previous stop_time, this is the initial station which cannot be traveled to 
    try:
        # i-1 is okay here, because the df is sorted 
        previous_stop_time = stop_times.loc[i-1]
    except KeyError:
        return False
    has_arrived_at_stop_time = current_time <= parseGtfsTimestringAsTimeObject(current_stop_time['arrival_realtime'])
    has_departed_previous_stop_time = current_time >= parseGtfsTimestringAsTimeObject(previous_stop_time['departure_realtime'])
    return has_arrived_at_stop_time and has_departed_previous_stop_time

def getPreviousStopId(stop_times, current_stop_time):
    trip_id = current_stop_time['trip_id']
    
    current_stop_sequence = current_stop_time['stop_sequence']
        
    previous_stop_sequence = current_stop_sequence - 1
    
    previous_stop_times = stop_times.loc[(stop_times['trip_id'] == trip_id) & (stop_times['stop_sequence'] == previous_stop_sequence)].reset_index(drop=True)
    
    if len(previous_stop_times) == 0:
         # if previous stop does not exist, train is coming from depot
        return 'DEPOT'

    previous_stop_time = previous_stop_times.iloc[0]

    return previous_stop_time['stop_id']

def getNextStopId(stop_times, current_stop_time):
    trip_id = current_stop_time['trip_id']
    
    current_stop_sequence = current_stop_time['stop_sequence']
        
    next_stop_sequence = current_stop_sequence + 1
    next_stop_times = stop_times.loc[(stop_times['trip_id'] == trip_id) & (stop_times['stop_sequence'] == next_stop_sequence)].reset_index(drop=True)
    
    if len(next_stop_times) == 0:
        # if previous stop does not exist, train is coming from depot
        return 'DEPOT'
    
    next_stop_time = next_stop_times.iloc[0]

    return next_stop_time['stop_id']

def getStopName(stops, stop_id):
    if stop_id == 'DEPOT':
        return 'DEPOT'
   # should be 1 or 0
    applicable_stops = stops.loc[stops['stop_id'] == stop_id]
    if len(applicable_stops) == 0:
        # stop not found
        return 'ERROR'
    else:
        applicable_stop = applicable_stops.iloc[0]
        return f"{applicable_stop['stop_name']} (Steig {applicable_stop['platform_code']})"

status_df = pd.DataFrame()

for i, active_trip in trips.iterrows():
    trip_id = active_trip['trip_id']

    stop_times_for_this_trip = stop_times.loc[stop_times['trip_id'] == trip_id]

    # find stops, at which the vehicle is currently stopped (should be 0 or 1)
    # vehicle is stopped, if current time is between arrival and departure of a stop
    stop_times_stopped_at = [stop_time for _,stop_time in stop_times_for_this_trip.iterrows() if isStoppedAtStopTime(stop_time)]

    # find stops that the vehicle is currently traveling to (should be 0 or 1)
    # vehicle is traveling to a stop if it has not arrived a stop but already departed the previous stop
    stop_times_traveling_to = [stop_time for i ,stop_time in stop_times_for_this_trip.iterrows() if isTravelingToStoptime(stop_times_for_this_trip, i)]

    #print(trip_id, len(stop_times_stopped_at), len(stop_times_traveling_to))

    status = ''
    previous_stop_id = ''
    current_stop_id = ''
    next_stop_id = ''
    current_stop_name= ''
    previous_stop_name=''
    statuscode = ''


    if len(stop_times_stopped_at) > 0:
        status = 'STOPPED_AT'
        current_stop_time = stop_times_stopped_at[0]

        previous_stop_id = getPreviousStopId(stop_times, current_stop_time)
        current_stop_id = current_stop_time['stop_id']
        next_stop_id = getNextStopId(stop_times, current_stop_time)


        previous_stop_name = getStopName(stops, previous_stop_id)
        current_stop_name = getStopName(stops, current_stop_id)
        next_stop_name = getStopName(stops, next_stop_id)

        statuscode = f"{previous_stop_id}_{current_stop_id}_{next_stop_id}"
        
    elif len(stop_times_traveling_to) > 0:
        status = 'IN_TRANSIT_TO'
        next_stop_time = stop_times_traveling_to[0]

        previous_stop_id = getPreviousStopId(stop_times, next_stop_time)
        next_stop_id = next_stop_time['stop_id']
        
        previous_stop_name = getStopName(stops, previous_stop_id)
        next_stop_name = getStopName(stops, next_stop_id)
        
        statuscode = f"{previous_stop_id}_{next_stop_id}"
    else: 
        status = 'ERROR'

  
   
        
    route_id = active_trip['route_id']
    route_color = routes.loc[routes['route_id'] == route_id]['route_color']
    
    status_df_row = pd.DataFrame({'trip_id': trip_id,'status': [status], 
                  'current_stop_id': [current_stop_id], 
                  'previous_stop_id': [previous_stop_id], 
                  'next_stop_id': [next_stop_id],
                  'current_stop_name': [current_stop_name], 
                  'previous_stop_name': [previous_stop_name],
                                 'route_color_hex': route_color, 'statuscode':statuscode})

    
    status_df = pd.concat([status_df, status_df_row], ignore_index=True)

print(status_df)


# ## 8. Convert status to LED Matrix
# To finally display the vehicles on our LED Matrix, we need to translate the statuses of the vehicles into LEDs.
# For this, we create a mapping as csv, which we read as pandas dataframe, that maps a status to LEDs.
# A status is encoded as \<previous_stop_id>\_\<current_stop_id>_<T (transit) / S (stopped at)>, e.g. 
# 427404_427504_S for STOPPED_AT Gadamerplatz Steig A, coming from Eppelheimer Terrasse (Steig B)  
# The LEDs are addressed by their respective X and Y coordinate on the Matrix.
# In the mapping a status is mapped to one or more LEDs. LEDs are separated by &. LEDs can be referenced by multiple statuses.
# Example: 
# When the train is stopped at Gadamerplatz Steig A coming from Eppelheimer Terrasse Steig B the LEDs x=0, y=0 and x=0, y=1 should light up.
# The csv would look as follows
# ```
# statuscode,      leds  
# 427404_427504_S, 0-0&0-1
# ```
# 
# The LED matrix is represented in a pandas dataframe with the cell \[x,y] representing the LED at x,y in the matrix. The cell value is the HEX color(s) that the LED should display.  
# A cell value is either
# - 000000 => no light
# - single HEX-code (e.g. "FDC300") => static light FDC300
# - multiple HEX-codes separated by & (e.g. "FDC300&B10346") => light switching from FDC300 to B10346, indicating multiple vehicles on the same track
# 
# This dataframe / csv is the final output of this notebook and will be the input for the script that directly controls the LED matrix.  
# To continue the example above, the output matrix, assuming the route color is FDC300 would be  
# None,0     ,1,2...  
# 0   ,FDC300, ,  
# 1   ,FDC300, ,  
# 2   ,      , ,  
# ...  
# 
# 
# 
# 

# In[110]:


import pandas as pd
import numpy as np

# import mapping
statuscode_led_mapping = pd.read_csv('./statuscode_led_mapping.csv', sep=';')

# create led_matrix dataframe with all led colors set to black
led_matrix = pd.DataFrame(np.full((32,64), "000000"))



# iterate over status_df rows and display them
for _, status_row in status_df.iterrows():
    statuscode = status_row['statuscode']
    route_color_hex = status_row['route_color_hex']

    # get corresponding led coordinates from mapping
    # if a statuscode occurs on more than one route, more than 1 mapping row will be found, but as a statuscode is always displayed on the same leds
    # all rows will contain the same led coordinates
    applicable_mapping_rows = statuscode_led_mapping[statuscode_led_mapping['statuscode'] == statuscode]
    if len(applicable_mapping_rows) == 0:
        # statuscode not in mapping yet
        print(f"skipping statuscode {statuscode}") 
        continue
    
    statuscode_led_mapping_row = applicable_mapping_rows.loc[applicable_mapping_rows.index[0]]

    led_mapping_string = statuscode_led_mapping_row['leds']
    leds_xy = led_mapping_string.split("&")
    for led_xy in leds_xy:
        x, y = led_xy.split("-")
        print(f"lighting led at x={x} and y={y}")
        # .at works with [row (y), col(x)]
        led_matrix.at[int(y),int(x)] = route_color_hex

# header None so that column index and row index type are int on import and we can use [int][int] to locate datapoints
led_matrix.to_csv('./led-matrix.csv', header=None, index=False)

led_matrix_read = pd.read_csv('./led-matrix.csv',header=None, dtype=str, index_col=None)
print(led_matrix_read[0][0] == "000000")
led_matrix_read.at[0,0] = "BCD300"


