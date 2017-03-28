import json
import requests
from requests.auth import HTTPBasicAuth
import time
import datetime
import csv
from subprocess import Popen, PIPE, STDOUT
import MySQLdb
import MySQLdb.converters
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

# MailChimp API v3.0 HTTP Simple Auth Credentials
un = config.mailchimp_api_user
pw = config.mailchimp_api_pass

# Set Initial Offset of O
member_offset = 0

# MailChimp API v3.0 Parameters to Get Subscribed Users over designated time window by paginating via "offset" parameter.
# Grabs in batches of 1000 to be nice to API. Can modify based on query times.
info = {'status':'subscribed',
        'since_timestamp_opt':datetime.datetime.utcfromtimestamp(origin_time).isoformat(),
        'before_timestamp_opt':datetime.datetime.utcfromtimestamp(time_now).isoformat(),
        'count':1000, 'fields':'members.email_address,members.timestamp_opt,members.status,members.stats,members.list_id,members.location',
        'offset':member_offset}
# Initialize Empty List for Total Members
total_members = []

# Get first batch of users
r = requests.get('https://us4.api.mailchimp.com/3.0/lists/f2fab1dfd4/members',auth=HTTPBasicAuth(un, pw), params=info)
member_array = r.json()

# Iterate over entire list set till no more members are returned.
while (len(member_array['members'])) > 1:
    total_members.append(member_array['members'])
    print("Total Members in Array is currently %s" % (len(total_members) * 1000))
    member_offset += 1000
    info = {'status':'subscribed',
            'since_timestamp_opt':datetime.datetime.utcfromtimestamp(origin_time).isoformat(),
            'before_timestamp_opt':datetime.datetime.utcfromtimestamp(time_now).isoformat(),
            'count':1000, 'fields':'members.email_address,members.timestamp_opt,members.status,members.stats,members.list_id,members.location',
            'offset':member_offset}
    r = requests.get('https://us4.api.mailchimp.com/3.0/lists/f2fab1dfd4/members',auth=HTTPBasicAuth(un, pw), params=info)
    member_array = r.json()

# Setup DB Connnection
# Moved from earlier in script to not have open MySQL connection that can timeout if
# API scraping takes a long time. Have seen timeouts happen.
conv_dict = MySQLdb.converters.conversions.copy()
conv_dict[246]=float
conv_dict[3]=int

ca_settings = {'ca': '/home/quasar/rds-combined-ca-bundle.pem'}
db = MySQLdb.connect(host=config.host,  # hostname
                     user=config.user,  # username
                     passwd=config.pw,  # password
                     use_unicode=True,  # Use unicode for queries.
                     charset='utf8',    # Use UTF8 character set for queries.
                     conv=conv_dict)    # datatype conversions

cur = db.cursor()

# Iterate over entire member array and add to DB
for member in total_members:
    for submember in member:
        row_count = cur.execute("SELECT northstar_id FROM quasar.users \
                                   WHERE email = %s",(submember['email_address'],))
        if row_count < 1:
            pass
        else:
            northstar_id = cur.fetchall()
            first_subscribed = submember['timestamp_opt'].replace('T',' ').split('+')[0]
            status = submember['status']
            list_id = submember['list_id']
            avg_open_rate = submember['stats']['avg_open_rate']
            avg_click_rate = submember['stats']['avg_click_rate']
            latitude = submember['location']['latitude']
            longitude = submember['location']['longitude']
            country_code = submember['location']['country_code']
            cur.execute("REPLACE INTO quasar.users (northstar_id,\
                        mailchimp_first_subscribed, mailchimp_subscription_status,\
                        mailchimp_list_id, mailchimp_avg_open_rate,\
                        mailchimp_avg_click_rate, mailchimp_latitude,\
                        mailchimp_longitude, mailchimp_country_code) VALUES(%s,%s,\
                        %s,%s,%s,%s,%s,%s,%s)",
                        (northstar_id,first_subscribed,status,list_id,avg_open_rate,
                        avg_click_rate,latitude,longitude,country_code))

# Double Check to Commit DB Changes and Close Connection in case of loop failure
db.commit()
cur.close()
db.close()

# Print How Long Run Took
end = time.time()
timer = end-start
print("Total processing time was %s seconds." % timer)
