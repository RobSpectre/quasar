import json
import requests
from requests.auth import HTTPBasicAuth
import time
import datetime
import csv
from subprocess import Popen, PIPE, STDOUT
import psycopg2
import sys
import config

# Set Backfill Hours, default of from supplied argument
if not sys.argv[1]:
    backfill_hours = 2
else:
    backfill_hours = int(sys.argv[1])

# This is to track time of script
start = time.time()

# Start time in Unixtime to use internally in ETL processing
time_now = time.time()
# Set backfill to seconds and set backfill origin time to time_now - backfill_time in seconds
backfill_time = backfill_hours * 3600
origin_time = time_now - backfill_time

# Set initial time Windows from current time
start_window_time = time_now - 7200
end_window_time = time_now

# MailChimp API v3.0 HTTP Simple Auth Credentials
un = config.mailchimp_api_user
pw = config.mailchimp_api_pass

# MailChimp API v3.0 Paremeters to Get Unsubscribed Users over 2 hour time window assuming max 10000 subscribers per time windows. Number guidelines
# set via conversations with Dee on max ingestion rate
info = {'status':'subscribed', 'since_timestamp_opt':datetime.datetime.utcfromtimestamp(start_window_time).isoformat() , 'before_timestamp_opt':datetime.datetime.utcfromtimestamp(end_window_time).isoformat() , 'count':10000, 'fields':'members.email_address,members.last_changed'}

# Initialize Empty List for Total Members
total_members = []

# Iterate over 2 hour time windows to build out large member array of unsubscribes
while (start_window_time - origin_time) >= -7200:
    r = requests.get('https://us4.api.mailchimp.com/3.0/lists/f2fab1dfd4/members',auth=HTTPBasicAuth(un, pw), params=info)
    member_array = r.json()
    if not member_array['members']:
        print("No subscribes for this time window!")
    else:
        total_members.append(member_array['members'])
        print("Total Members in Array is currently %s" % len(total_members))
    end_window_time = start_window_time
    start_window_time -= 7200
    info = {'status':'subscribed', 'since_timestamp_opt':datetime.datetime.utcfromtimestamp(start_window_time).isoformat() , 'before_timestamp_opt':datetime.datetime.utcfromtimestamp(end_window_time).isoformat() , 'count':10000, 'fields':'members.email_address,members.last_changed'}

db = psycopg2.connect(
          user=config.user,
          password=config.pw,
          dbname=config.db,
          host=config.host,
          port="5439",
          sslmode="require")

cur = db.cursor()

# Create staging table for changes
cur.execute("DROP TABLE IF EXISTS users_and_activities.mailchimp_sub_staging")
db.commit()
cur.execute("CREATE TABLE users_and_activities.mailchimp_sub_staging AS SELECT * FROM users_and_activities.mailchimp_sub")
db.commit()

# Iterate over entire member array and add to DB
for member in total_members:
    for submember in member:
        email = submember['email_address']
        confirm_time = submember['last_changed'].replace('T',' ').split('+')[0]
        cur.execute("INSERT into users_and_activities.mailchimp_sub_staging values (%s,%s)", (email,confirm_time))

db.commit()

# Clean-Up and Remove Duplicates - Placeholder workaround since RedShift doesn't strictly enforce Primary Keys

cur.execute("DELETE FROM users_and_activities.mailchimp_sub USING users_and_activities.mailchimp_sub_staging WHERE users_and_activities.mailchimp_sub.email_address = users_and_activities.mailchimp_sub_staging.email_address")
db.commit()
cur.execute("INSERT INTO users_and_activities.mailchimp_sub SELECT * FROM users_and_activities.mailchimp_sub_staging")
db.commit()
cur.execute("DROP TABLE users_and_activities.mailchimp_sub_staging")
db.commit()

# Close Connection in case of loop failure
cur.close()
db.close()

# Print How Long Run Took
end = time.time()
timer = end-start
print("Total processing time was %s seconds." % timer)
