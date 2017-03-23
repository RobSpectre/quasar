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

# Set pagination variable to be true by default. This
# will track whether there are any more pages in a
# result set. If there aren't, will return false.
nextPage = True


db = MySQLdb.connect(host=config.host,  # hostname
                     user=config.user,  # username
                     passwd=config.pw)  # password

db.set_character_set('utf8')
cur = db.cursor()
cur.execute('SET NAMES utf8;')
cur.execute('SET CHARACTER SET utf8;')
cur.execute('SET character_set_connection=utf8;')
"""Set UTF-8 encoding on MySQL connection."""

if len(sys.argv) < 2:
    cur.execute("SELECT * from quasar_etl_status.northstar_ingestion \
                 WHERE counter_name = 'last_page_scraped';")
    db.commit()
    last_page = cur.fetchall()
    i = last_page[0][1]
else:
    i = int(sys.argv[1])
"""Check if page start point provided, otherwise use last processed point."""


def to_string(base_value):
    """Converts to string and replaces values with NULL when blank or None."""
    base_string = str(base_value)
    strip_special_chars = re.sub(r'[()<>/"\'\\]', '', base_string)
    transform_null = re.sub(r'^$|\bNone\b', 'NULL', strip_special_chars)
    return str(transform_null)


while nextPage is True:
    current_page = ns_fetcher.getUsers(100, i)
    for user in current_page:
        query = "REPLACE INTO quasar.users (northstar_id,\
                                            northstar_created_at_timestamp,\
                                            drupal_uid,\
                                            northstar_id_source_name,\
                                            email, mobile, birthdate,\
                                            first_name, last_name,\
                                            addr_street1, addr_street2,\
                                            addr_city, addr_state,\
                                            addr_zip, country, language,\
                                            agg_id, cgg_id,\
                                            moco_commons_profile_id,\
                                            moco_current_status,\
                                            moco_source_detail)\
                                            VALUES(\"{0}\",\"{1}\",{2},\"{3}\",\
                                            \"{4}\",\"{5}\",\"{6}\",\"{7}\",\
                                            \"{8}\",\"{9}\",\"{10}\",\"{11}\",\
                                            \"{12}\",\"{13}\",\"{14}\",\"{15}\"\
                                            ,NULL,NULL,\"{16}\",\"{17}\",\
                                            \"{18}\")".format(
                                            to_string(user['id']),
                                            to_string(user['created_at']),
                                            to_string(user['drupal_id']),
                                            to_string(user['source']),
                                            to_string(user['email']),
                                            to_string(user['mobile']),
                                            to_string(user['birthdate']),
                                            to_string(user['first_name']),
                                            to_string(user['last_name']),
                                            to_string(user['addr_street1']),
                                            to_string(user['addr_street2']),
                                            to_string(user['addr_city']),
                                            to_string(user['addr_state']),
                                            to_string(user['addr_zip']),
                                            to_string(user['country']),
                                            to_string(user['language']),
                                            to_string(user['mobilecommons_id']),
                                            to_string(user['mobilecommons_status']),
                                            to_string(user['source_detail']))
        cur.execute(query)
        db.commit()
    nextPage = ns_fetcher.nextPageStatus(100, i)
    if nextPage is True:
        i += 1
        cur.execute("REPLACE INTO quasar_etl_status.northstar_ingestion \
                    (counter_name, counter_value) VALUES(\"last_page_scraped\",\
                    \"{0}\")".format(i))
        db.commit()
    else:
        current_page = ns_fetcher.getUsers(100, i)
        for user in current_page:
            query = "REPLACE INTO quasar.users (northstar_id,\
                                                northstar_created_at_timestamp,\
                                                drupal_uid,\
                                                northstar_id_source_name,\
                                                email, mobile, birthdate,\
                                                first_name, last_name,\
                                                addr_street1, addr_street2,\
                                                addr_city, addr_state,\
                                                addr_zip, country, language,\
                                                agg_id, cgg_id,\
                                                moco_commons_profile_id,\
                                                moco_current_status,\
                                                moco_source_detail)\
                                                VALUES(\"{0}\",\"{1}\",{2},\"{3}\",\
                                                \"{4}\",\"{5}\",\"{6}\",\"{7}\",\
                                                \"{8}\",\"{9}\",\"{10}\",\"{11}\",\
                                                \"{12}\",\"{13}\",\"{14}\",\"{15}\"\
                                                ,NULL,NULL,\"{16}\",\"{17}\",\
                                                \"{18}\")".format(
                                                to_string(user['id']),
                                                to_string(user['created_at']),
                                                to_string(user['drupal_id']),
                                                to_string(user['source']),
                                                to_string(user['email']),
                                                to_string(user['mobile']),
                                                to_string(user['birthdate']),
                                                to_string(user['first_name']),
                                                to_string(user['last_name']),
                                                to_string(user['addr_street1']),
                                                to_string(user['addr_street2']),
                                                to_string(user['addr_city']),
                                                to_string(user['addr_state']),
                                                to_string(user['addr_zip']),
                                                to_string(user['country']),
                                                to_string(user['language']),
                                                to_string(user['mobilecommons_id']),
                                                to_string(user['mobilecommons_status']),
                                                to_string(user['source_detail']))
            cur.execute(query)
            db.commit()

end_time = time.time()  # Record when script stopped running.
duration = end_time - start_time  # Total duration in seconds.
print('duration: ', duration)
