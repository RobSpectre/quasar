import MySQLdb
import config
import time
import re
import sys
from DSNorthstarScraper import NorthstarScraper

"""DS Northstar to Quasar User ETL script.

This ETL scripts scrapes the DoSomething Northstar User API and ETL's the
output to our MySQL Quasar data warehouse.

The script takes an optional argument for what Northstar page result to start
on. This is mostly used to backfill from a certain page, or from the dawn
of time. Otherwise, pagination is stored in an small status tracking table
that gets updated on ingestion loop.

"""

start_time = time.time()
"""Keep track of start time of script."""

ns_fetcher = NorthstarScraper()

ns_member_counter = ns_fetcher.userCount()
ns_pages = ns_member_counter[1]
"""Get current max page number."""

db = MySQLdb.connect(host=config.host, #hostname
          user=config.user, #  username
          passwd=config.pw) # password

db.set_character_set('utf8')
cur = db.cursor()
cur.execute('SET NAMES utf8;')
cur.execute('SET CHARACTER SET utf8;')
cur.execute('SET character_set_connection=utf8;')
"""Set UTF-8 encoding on MySQL connection."""

if len(sys.argv) < 2:
    cur.execute("SELECT * from quasar_etl_status.northstar_ingestion WHERE counter_name = 'last_page_scraped';")
    db.commit()
    last_page = cur.fetchall()
    i = last_page[0][1]
else:
    i = int(sys.argv[1])
"""Check if page starting point provided, otherwise use last processed point."""

def ts(base_value):
    """Converts to string and replaces values with NULL when blank or None."""
    base_string = str(base_value)
    strip_special_chars = re.sub(r'[()<>/"\'\\]','',base_string)
    transform_null = re.sub(r'^$|\bNone\b','NULL',strip_special_chars)
    transformed = str(transform_null)
    return transformed

while i <= ns_pages:
    current_page = ns_fetcher.getUsers(100, i)
    for user in current_page:
        query = "REPLACE INTO quasar.users (northstar_id, northstar_created_at_timestamp, drupal_uid, \
                                                northstar_id_source_name, email, mobile, birthdate, first_name, \
                                                last_name, addr_street1, addr_street2, addr_city, addr_state, \
                                                addr_zip, country, language, agg_id, cgg_id) \
                                                VALUES(\"{0}\",\"{1}\",{2},\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\",\"{8}\",\
                                                \"{9}\",\"{10}\",\"{11}\",\"{12}\",\"{13}\",\"{14}\",\"{15}\",NULL,NULL)".format(\
                                                ts(user['id']),ts(user['created_at']),ts(user['drupal_id']),ts(user['source']), \
                                                ts(user['email']),ts(user['mobile']),ts(user['birthdate']),ts(user['first_name']),\
                                                ts(user['last_name']),ts(user['addr_street1']),ts(user['addr_street2']), \
                                                ts(user['addr_city']),ts(user['addr_state']),ts(user['addr_zip']),\
                                                ts(user['country']),ts(user['language']))
        print(query)
        cur.execute(query)
        db.commit()
    i+=1
    cur.execute("REPLACE INTO quasar_etl_status.northstar_ingestion (counter_name, counter_value) VALUES(\"last_page_scraped\", \"{0}\")".format(i))
    db.commit()

end_time = time.time()  # Record when script stopped running.
duration = end_time - start_time # Total duration in seconds.
print ('duration: ', duration)
