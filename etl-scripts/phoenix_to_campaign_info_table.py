import MySQLdb
import config
import time
import sys
import re
from DSPhoenixWebScraper import PhoenixScraper

start_time = time.time()
"""Keep track of start time of script."""

phoenix_fetcher = PhoenixScraper()

total_pages = phoenix_fetcher.getPages()
page_number = 1

phoenix_campaigns = phoenix_fetcher.getCampaigns()

db = MySQLdb.connect(host=config.host,  # hostname
                     user=config.user,  # username
                     passwd=config.pw)  # password

db.set_character_set('utf8')
cur = db.cursor()
cur.execute('SET NAMES utf8;')
cur.execute('SET CHARACTER SET utf8;')
cur.execute('SET character_set_connection=utf8;')
"""Set UTF-8 encoding on MySQL connection."""


def null_cleanup(base_value):
    """Replaces None in campaign field values."""
    transform_null = {}
    transform_null['id'] = base_value['id']
    transform_null['title'] = base_value['title']
    transform_null['type'] = base_value['type']
    transform_null['language'] = base_value['language']
    transform_null['campaign_runs'] = base_value['campaign_runs']
    transform_null['created_at'] = base_value['created_at']
    if base_value['action_types']['primary'] is None:
        transform_null['action_types'] = {'primary': {'name': 'NULL'}}
    else:
        transform_null['action_types'] = base_value['action_types']
    transform_null['tagline'] = base_value['tagline']
    if base_value['reportback_info']['noun'] is None:
        transform_null['reportback_info'] = {'verb': 'NULL', 'noun': 'NULL'}
    else:
        transform_null['reportback_info'] = base_value['reportback_info']
    return transform_null


def to_string(base_value):
    """Converts to string and replaces values with NULL when blank or None."""
    if base_value is not None:
        base_string = str(base_value)
        strip_special_chars = re.sub(r'[()<>/"\'\\]', '', base_string)
        transform_null = re.sub(r'^$|\bNone\b', 'NULL', strip_special_chars)
        return str(transform_null)
    else:
        blank_value = "NULL"
        return blank_value

while page_number <= total_pages:
    phoenix_campaigns = phoenix_fetcher.getCampaigns(page_number)
    for i in phoenix_campaigns:
        k = null_cleanup(i)  # Cleanup Campaign to have no empty values.
        for j in k['campaign_runs']:
            for lang in k['campaign_runs'][j]:
                run_id = k['campaign_runs'][j][lang]['id']
                query = "REPLACE INTO quasar.campaign_info \
                         (campaign_node_id,\
                          campaign_node_id_title,\
                          campaign_run_id,\
                          campaign_type,\
                          campaign_language,\
                          campaign_created_date,\
                          campaign_action_type,\
                          campaign_cta,\
                          campaign_noun,\
                          campaign_verb)\
                          VALUES({0},\"{1}\",\
                          {2},\"{3}\",\"{4}\",\
                          from_unixtime({5}),\"{6}\",\
                          \"{7}\", \"{8}\",\
                          \"{9}\")".format(
                          k['id'],
                          to_string(k['title']),
                          run_id,
                          to_string(k['type']),
                          to_string(k['language']['language_code']),
                          k['created_at'],
                          to_string(k['action_types']['primary']['name']),
                          to_string(k['tagline']),
                          to_string(k['reportback_info']['noun']),
                          to_string(k['reportback_info']['verb'])
                          )
                cur.execute(query)
                db.commit()

    page_number += 1

end_time = time.time()  # Record when script stopped running.
duration = end_time - start_time  # Total duration in seconds.
print('duration: ', duration)
