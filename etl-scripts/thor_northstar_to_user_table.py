import MySQLdb
import config
import time
import re
import sys
from DSNorthstarScraper import NorthstarScraper

"""DS Northstar to Quasar User ETL QA script.

This ETL scripts scrapes the DoSomething Thor Northstar User API and ETL's the
output to our MySQL Quasar data warehouse.

The script takes an optional argument for what Northstar page result to start
on. This is mostly used to backfill from a certain page, or from the dawn
of time. Otherwise, pagination is stored in an small status tracking table
that gets updated on ingestion loop.

"""

start_time = time.time()
"""Keep track of start time of script."""


# Set pagination variable to be true by default. This
# will track whether there are any more pages in a
# result set. If there aren't, will return false.
nextPage = True

ca_settings = {'ca': '/home/quasar/rds-combined-ca-bundle.pem'}
db = MySQLdb.connect(host=config.host,  # hostname
                     user=config.user,  # username
                     passwd=config.pw,  # password
                     use_unicode=True,  # Use unicode for queries.
                     charset='utf8',    # Use UTF8 character set for queries.
                     ssl=ca_settings)   # Connect using SSL

cur = db.cursor()

if len(sys.argv) == 3:
    if sys.argv[1] == 'prod':
        northstar_env_url = 'https://northstar.dosomething.org'
        db_env = 'users'
    elif sys.argv[1] == 'thor':
        northstar_env_url = 'https://northstar-thor.dosomething.org'
        db_env = 'thor_users'
    else:
        print("Please provide a working Northstar environment: thor/prod.")
        sys.exit(0)
    if sys.argv[2] == 'cont':
       if sys.argv[1] == 'prod':
          cur.execute("SELECT * from quasar_etl_status.northstar_ingestion \
                       WHERE counter_name = 'last_page_scraped'")
          db.commit()
          last_page = cur.fetchall()
          i = last_page[0][1]
       elif sys.argv[1] == 'thor':
          cur.execute("SELECT * from quasar_etl_status.thor_northstar_ingestion \
                       WHERE counter_name = 'last_page_scraped'")
          db.commit()
          last_page = cur.fetchall()
          i = last_page[0][1]
       else:
          print("Can not continue, invalid env specified.")
    elif isinstance(sys.argv[2], int):
        i = int(sys.argv[2])
    else:
        print("Please input 'cont' to continue backfill or integer value.")
else:
    print("Sorry, please specify proper arguments. i.e. env/page")
"""Determine environment, and page to start from.

With no arguments, env is set to prod and ingestion begins from last page.
With 1 argument, env is set to prod and ingestion begins at arg1 page.
With 2 arguments, env is set to arg1 and ingestion begins at arg2 page.

"""

# Set environment url for Northstar, prod or thor.
ns_fetcher = NorthstarScraper(northstar_env_url)


def to_string(base_value):
    """Converts to string and replaces None values with empty values."""
    if base_value is None:
        return None
    else:
        base_string = str(base_value)
        strip_special_chars = re.sub(r'[()<>/"\'\\]', '', base_string)
        return str(strip_special_chars)

while nextPage is True:
    current_page = ns_fetcher.getUsers(100, i)
    for user in current_page:
        cur.execute("REPLACE INTO quasar.thor_users (northstar_id,\
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
                                            VALUES(%s,%s,%s,%s,\
                                            %s,%s,%s,%s,\
                                            %s,%s,%s,%s,\
                                            %s,%s,%s,%s,\
                                            NULL,NULL,%s,%s,%s)",
                                            (to_string(user['id']),
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
                                            to_string(user['source_detail'])))
        db.commit()
    nextPage = ns_fetcher.nextPageStatus(100, i)
    if nextPage is True:
        i += 1
        cur.execute("REPLACE INTO quasar_etl_status.thor_northstar_ingestion \
                (counter_name, counter_value) VALUES(\"last_page_scraped\",\
                \"{0}\")".format(i))
        db.commit()
    else:
        current_page = ns_fetcher.getUsers(100, i)
        for user in current_page:
            cur.execute("REPLACE INTO quasar.thor_users (northstar_id,\
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
                                                VALUES(%s,%s,%s,%s,\
                                                %s,%s,%s,%s,\
                                                %s,%s,%s,%s,\
                                                %s,%s,%s,%s,\
                                                NULL,NULL,%s,%s,%s)",
                                                (to_string(user['id']),
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
                                                to_string(user['source_detail'])))
            db.commit()

cur.close()
db.close()

end_time = time.time()  # Record when script stopped running.
duration = end_time - start_time  # Total duration in seconds.
print('duration: ', duration)
