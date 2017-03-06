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

runs = {}

db = MySQLdb.connect(host=config.host,  # hostname
                     user=config.user,  # username
                     passwd=config.pw)  # password

db.set_character_set('utf8')
cur = db.cursor()
cur.execute('SET NAMES utf8;')
cur.execute('SET CHARACTER SET utf8;')
cur.execute('SET character_set_connection=utf8;')
"""Set UTF-8 encoding on MySQL connection."""

def to_string(base_value):
    """Converts to string and replaces values with NULL when blank or None."""
    base_string = str(base_value)
    strip_special_chars = re.sub(r'[()<>/"\'\\]', '', base_string)
    transform_null = re.sub(r'^$|\bNone\b', 'NULL', strip_special_chars)
    return str(transform_null)

while page_number <= total_pages:
    phoenix_campaigns = phoenix_fetcher.getCampaigns(page_number)
    for i in phoenix_campaigns:
        for j in i['campaign_runs']:
            for lang in i['campaign_runs'][j]:
                run_id = i['campaign_runs'][j][lang]['id']
                print(i['id'])
                print(i['action_types']['primary'])
                print(i['reportback_info'])
                all(i)
                if i['action_types']['primary'] is not None or i['reportback_info']['verb'] is not None:
                    query = "REPLACE INTO quasar.campaign_info (campaign_node_id,\
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
                                                                {5},\"{6}\",\
                                                                \"{7}\", \"{8}\",\
                                                                \"{9}\")".format(
                                                                i['id'],
                                                                to_string(i['title']),
                                                                run_id,
                                                                to_string(i['type']),
                                                                to_string(i['language']['language_code']),
                                                                i['created_at'],
                                                                to_string(i['action_types']['primary']['name']),
                                                                to_string(i['tagline']),
                                                                to_string(i['reportback_info']['noun']),
                                                                to_string(i['reportback_info']['verb']))
                else:
                    query = "REPLACE INTO quasar.campaign_info (campaign_node_id,\
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
                                                                {5},NULL,\
                                                                NULL,NULL,\
                                                                NULL)".format(
                                                                i['id'],
                                                                to_string(i['title']),
                                                                run_id,
                                                                to_string(i['type']),
                                                                to_string(i['language']['language_code']),
                                                                i['created_at'])
                cur.execute(query)
                db.commit()
                                                            
    page_number += 1
