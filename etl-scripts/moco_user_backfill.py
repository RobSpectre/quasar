import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from datetime import datetime, date, timedelta
from bs4 import BeautifulSoup
import sys
import config
import MySQLdb
import MySQLdb.converters

### Create DB Connection with Appropriate Conversions
#   Conversions inherited from Josh's other ETL scripts.
conv_dict = MySQLdb.converters.conversions.copy()
conv_dict[246]=float
conv_dict[8]=int
#open connection
db = MySQLdb.connect(host=config.host, #hostname
      user=config.user, #  username
      passwd=config.pw, #  password
      conv=conv_dict) # datatype conversions
      #set cursor object, and set to dict dursor
cur = db.cursor(MySQLdb.cursors.DictCursor)

### Time Conversion from Now to ISO 8601 format used by MC
now = datetime.now()
now_iso = now.isoformat().replace("-", "").replace(":", "").split(".")
now_iso = now_iso[0]

### Break down total time backfill to go based on current execute time of scripts
origin_time = now - timedelta(hours=int(sys.argv[1]))
origin_time_iso = origin_time.isoformat().replace("-", "").replace(":", "").split(".")
origin_time_iso = origin_time_iso[0]

# Print number of hours backfill
print("Backfilling mobile users by " + sys.argv[1] + " hours.")

##
### Set Initial Page and Limit Vars
limit_num = 300
page_num = 0

while limit_num == 300:
    page_num += 1
    # Setup Mobile Commons API Payload and Request
    mob_com_api_profile_req = requests.Session()
    retries = Retry(total=6, backoff_time=1.9)
    mob_com_api_profile_req.mount('https://', HTTPAdapter(max_retries=retries))
    mob_com_profile_payload = { 'from' : origin_time_iso, 'to' : now_iso , 'limit': '300', 'page' : page_num }
    mob_com_api_profile_req = requests.get('https://secure.mcommons.com/api/profiles', params=mob_com_profile_payload, auth=(config.mc_user,config.mc_pw))
    # Capture output as BeautifulSoup object, and iterate page till "num" does not equal 300 (default page limit set by script)
    profile_parse = BeautifulSoup(mob_com_api_profile_req.text, 'xml')
    # Set Num of profile for next run-through
    limit_num = int(profile_parse.profiles.get('num'))
    profiles = profile_parse.find_all('profile')

    # Iterate through each profile and insert into DB
    for profile in profiles:
        phone_number = profile.phone_number.text
        created_at = profile.created_at.text.replace(" UTC", "")
        source_type = profile.source.get('type')
        source_name = profile.source.get('name')
        status = profile.status.text
        if len(phone_number.strip()) == 11 and phone_number.startswith('1'):
           us_phone_number = phone_number[1:11]
        else:
           us_phone_number = "NULL"
        if source_type == "Opt-In Path":
            opt_in_path_id = profile.source.get('id')
        else:
            opt_in_path_id = "NULL"
        insert_profile = "replace into quasars.all_moco_users VALUES ({0}, {1}, {2}, \"{3}\", \"{4}\", {5}, \"{6}\")".format(phone_number, us_phone_number, created_at, source_type, source_name, opt_in_path_id, status)
        cur.execute(insert_profile)
        db.commit()
