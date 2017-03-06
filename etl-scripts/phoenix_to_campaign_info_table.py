# import MySQLdb
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

#db = MySQLdb.connect(host=config.host,  # hostname
#                     user=config.user,  # username
#                     passwd=config.pw)  # password

#db.set_character_set('utf8')
#cur = db.cursor()
#cur.execute('SET NAMES utf8;')
#cur.execute('SET CHARACTER SET utf8;')
#cur.execute('SET character_set_connection=utf8;')
"""Set UTF-8 encoding on MySQL connection."""

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
#        id = i['id']
#        runs[id] = []
        for j in i['campaign_runs']:
            for lang in i['campaign_runs'][j]:
                run_id = i['campaign_runs'][j][lang]['id']
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
                                                            to_string(i['reportback_info']['verb'])
                                                            )
                #if (runs[id].count(run_id) == 0):
                #    runs[id].append(run_id)
                #print("Current runs are:", run_id)
    # for run in runs:
    #    print(run)
    page_number += 1

#print(runs)
