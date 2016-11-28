import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import re
from bs4 import BeautifulSoup
import sys
import config
import MySQLdb
import MySQLdb.converters

### Put together Mobile Commons API Request
mob_com_api_req = requests.Session()
retries = Retry(total=6, backoff_factor=600)
mob_com_api_req.mount('https://', HTTPAdapter(max_retries=retries))
mob_com_api_req = requests.get('https://secure.mcommons.com/api/campaigns?include_opt_in_paths=1', auth=(config.mc_user,config.mc_pw))

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


### Capture Output into Beautiful Soup
mob_com_campaign_soup = BeautifulSoup(mob_com_api_req.text, 'xml')

# Assign Individual Campaigns to list
mob_com_campaigns = mob_com_campaign_soup.find_all('campaign')

# Regex Checker for Campaign Description with Format: Int OR Int,Int
nid_run_id = re.compile('^(\d{1,6})(?:, ?(\d{1,6}))?')

# Iterate through all Mobile Commons Campaigns and populate "mobile_campaign_id_lookup" table. opt_in_path is the Primary Key to the table.
for campaign in mob_com_campaigns:
   print("****************************")
   name = campaign.find('name')
   description = campaign.find('description')
   campaign_id = campaign.get('id')
   opt_in_path = campaign.find_all('opt_in_path')
   for path in opt_in_path:
        path_name = path.find('name')
        matches = nid_run_id.match(description.text)
        if matches is not None:
             nid = matches.group(1)
             run_id = matches.group(2)
             if run_id is not None:
                  insert_campaign = "replace into users_and_activities.mobile_campaign_id_lookup VALUES ({0}, \"{1}\", {2}, \"{3}\", {4}, {5})".format(path.get('id'), path_name.text.replace("\"", ""), campaign_id, name.text.replace("\"", ""), nid, run_id)
                  print (insert_campaign)
                  cur.execute(insert_campaign)
                  db.commit()
             else:
                  insert_campaign = "replace into users_and_activities.mobile_campaign_id_lookup VALUES ({0}, \"{1}\", {2}, \"{3}\", {4}, {5})".format(path.get('id'), path_name.text.replace("\"", ""), campaign_id, name.text.replace("\"", ""), nid, 'NULL')
                  print (insert_campaign)
                  cur.execute(insert_campaign)
                  db.commit()
        else:
             insert_campaign = "replace into users_and_activities.mobile_campaign_id_lookup VALUES ({0}, \"{1}\", {2}, \"{3}\", {4}, {5})".format(path.get('id'), path_name.text.replace("\"", ""), campaign_id, name.text.replace("\"", ""), 'NULL', 'NULL')
             print (insert_campaign)
             cur.execute(insert_campaign)
             db.commit()
   print("****************************")

cur.close()
db.close()
