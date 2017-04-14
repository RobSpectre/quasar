import requests
from requests.auth import HTTPBasicAuth
import time
from datetime import datetime
import MySQLdb
import MySQLdb.converters
import sys
import config
import hashlib

def isInt(s):
    """Check if value is type int and return boolean result.
    Source at http://stackoverflow.com/questions/1265665/python-check-if-a-string-represents-an-int-without-using-try-except
    """
    try:
        int(s)
        return True
    except ValueError:
        return False

def computeMD5hash(string):
    """Output MD5 hash of string."""
    m = hashlib.md5()
    m.update(string.encode('utf'))
    return m.hexdigest()

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

# Start time in Unixtime to use internally in ETL processing
time_now = time.time()

cur.execute("SELECT email FROM quasar.user_email_no_status")
db.commit()
member_count = cur.rowcount
total_members = cur.fetchall()

for i in total_members:
    print(i[0])
    user_md5 = computeMD5hash(i[0])
    r = requests.get('https://us4.api.mailchimp.com/3.0/lists/f2fab1dfd4/members/'\
                   + user_md5, auth=HTTPBasicAuth(un, pw))
    if r.status_code == 200:
        member = r.json()
        cur.execute("SELECT northstar_id FROM quasar.users \
                    WHERE email = %s", (i[0],))
        northstar_id = cur.fetchone()
        first_subscribed = member['timestamp_opt'].replace('T', ' ').split('+')[0]
        status = member['status']
        list_id = member['list_id']
        avg_open_rate = member['stats']['avg_open_rate']
        avg_click_rate = member['stats']['avg_click_rate']
        latitude = member['location']['latitude']
        longitude = member['location']['longitude']
        country_code = member['location']['country_code']
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
        print("Status updated for %s" % i[0])
    else:
        p = requests.get('https://us4.api.mailchimp.com/3.0/lists/8e7844f6dd/members/'\
                         + user_md5, auth=HTTPBasicAuth(un, pw))
        if p.status_code == 200:
            member = p.json()
            cur.execute("SELECT northstar_id FROM quasar.users \
                        WHERE email = %s", (i[0],))
            northstar_id = cur.fetchone()
            first_subscribed = member['timestamp_opt'].replace('T', ' ').split('+')[0]
            status = member['status']
            list_id = member['list_id']
            avg_open_rate = member['stats']['avg_open_rate']
            avg_click_rate = member['stats']['avg_click_rate']
            latitude = member['location']['latitude']
            longitude = member['location']['longitude']
            country_code = member['location']['country_code']
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
            print("Status updated for %s" % i[0])
        else:
            print("No status, WTF?")

# Commit DB Changes and Close Connection in case of loop failure
cur.close()
db.close()

# Print How Long Run Took
end = time.time()
timer = end-start
print("Total processing time was %s seconds." % timer)
