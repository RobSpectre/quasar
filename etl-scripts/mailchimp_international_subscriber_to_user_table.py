import requests
from requests.auth import HTTPBasicAuth
import time
from datetime import datetime
import MySQLdb
import MySQLdb.converters
import sys
import config


def isInt(s):
    """Check if value is type int and return boolean result.
    Source at http://stackoverflow.com/questions/1265665/python-check-if-a-string-represents-an-int-without-using-try-except
    """
    try:
        int(s)
        return True
    except ValueError:
        return False

mc_increment = 1000

# This is to track time of script
start = time.time()

# MailChimp API v3.0 HTTP Simple Auth Credentials
un = config.mailchimp_api_user
pw = config.mailchimp_api_pass

conv_dict = MySQLdb.converters.conversions.copy()
conv_dict[246] = float
conv_dict[3] = int

# Setup DB Connnection
# Moved from earlier in script to not
# have open MySQL connection that can timeout if
# API scraping takes a long time. Have seen timeouts happen.

ca_settings = {'ca': '/home/quasar/rds-combined-ca-bundle.pem'}
db = MySQLdb.connect(host=config.host,  # hostname
                     user=config.user,  # username
                     passwd=config.pw,  # password
                     use_unicode=True,  # Use unicode for queries.
                     charset='utf8',    # Use UTF8 character set for queries.
                     ssl=ca_settings,   # Connect using SSL
                     conv=conv_dict)    # datatype conversions

cur = db.cursor()

if len(sys.argv) == 3:
    if isInt(sys.argv[1]):
        backfill_hours = int(sys.argv[1])
        print("Backfilling by {0} hours".format(sys.argv[1]))
    else:
        print("Please provide number of hours to backfill.")
        sys.exit(0)
# Continue from last known offset or the beginning of hours.
    if sys.argv[2] == 'cont':
        cur.execute("SELECT * from quasar_etl_status.northstar_ingestion \
                 WHERE counter_name = 'mailchimp_member_offset'")
        db.commit()
        last_offset = cur.fetchall()
        member_offset = last_offset[0][1]
        print("Offset is {0} members".format(member_offset))
    elif isInt(sys.argv[2]):
        member_offset = int(sys.argv[2])
        print("Offset is {0} members".format(sys.argv[2]))
    else:
        print("Please provide valid offset.")
        sys.exit(0)
else:
    print("Please provide valid arguments for backfill hours/offset.")
    sys.exit(0)

# Start time in Unixtime to use internally in ETL processing
time_now = time.time()
# Set backfill to seconds
backfill_time = backfill_hours * 3600
# Set backfill origin time to time_now - backfill_time in seconds
origin_time = time_now - backfill_time

# MailChimp API v3.0 Parameters to Get Subscribed Users
# over designated time window by paginating via "offset" parameter.
# Grabs in batches of 1000 to be nice to API. Can modify based on query times.
info = {'status': 'subscribed',
        'since_timestamp_opt': datetime.utcfromtimestamp(origin_time).isoformat(),
        'before_timestamp_opt': datetime.utcfromtimestamp(time_now).isoformat(),
        'count': mc_increment, 'fields': 'members.email_address,members.timestamp_opt,members.status,members.stats,members.list_id,members.location',
        'offset': member_offset}
# Initialize Empty List for Total Members
total_members = []


# Get first batch of users
r = requests.get('https://us4.api.mailchimp.com/3.0/lists/8e7844f6dd/members',
                 auth=HTTPBasicAuth(un, pw), params=info)
member_array = r.json()

# Iterate over entire list set till no more members are returned.
while (len(member_array['members'])) > 1:
    total_members.append(member_array['members'])
    for member in total_members:
        print("Processing next page. Up to %s processed." % member_offset)
        for submember in member:
            cur.execute("SELECT northstar_id FROM quasar.users \
                        WHERE email = %s", (submember['email_address'],))
            northstar_email_matches = cur.rowcount
            if northstar_email_matches < 1:
                pass
            else:
                for x in range(0, northstar_email_matches):
                    northstar_id = cur.fetchone()
                    first_subscribed = submember['timestamp_opt'].replace('T', ' ').split('+')[0]
                    status = submember['status']
                    list_id = submember['list_id']
                    avg_open_rate = submember['stats']['avg_open_rate']
                    avg_click_rate = submember['stats']['avg_click_rate']
                    latitude = submember['location']['latitude']
                    longitude = submember['location']['longitude']
                    country_code = submember['location']['country_code']
                    cur.execute("UPDATE quasar.users SET mailchimp_first_subscribed = %s, \
                                mailchimp_subscription_status = %s,\
                                mailchimp_list_id = %s, mailchimp_avg_open_rate = %s,\
                                mailchimp_avg_click_rate = %s, mailchimp_latitude = %s,\
                                mailchimp_longitude = %s, mailchimp_country_code = %s \
                                WHERE northstar_id = %s",
                                (first_subscribed, status,
                                 list_id, avg_open_rate, avg_click_rate,
                                 latitude, longitude, country_code,
                                 northstar_id))
                    db.commit()
        total_members = []
    member_offset += mc_increment
    cur.execute("REPLACE INTO quasar_etl_status.northstar_ingestion \
                 (counter_name, counter_value) VALUES(\"mailchimp_member_offset\",\
                 \"{0}\")".format(member_offset))
    db.commit()
    info = {'status': 'subscribed',
            'since_timestamp_opt': datetime.utcfromtimestamp(origin_time).isoformat(),
            'before_timestamp_opt': datetime.utcfromtimestamp(time_now).isoformat(),
            'count': mc_increment,
            'fields': 'members.email_address,members.timestamp_opt,members.status,members.stats,members.list_id,members.location',
            'offset': member_offset}
    r = requests.get('https://us4.api.mailchimp.com/3.0/lists/8e7844f6dd/members',
                     auth=HTTPBasicAuth(un, pw), params=info)
    member_array = r.json()


# Commit DB Changes and Close Connection in case of loop failure
cur.close()
db.close()

# Print How Long Run Took
end = time.time()
timer = end-start
print("Total processing time was %s seconds." % timer)
