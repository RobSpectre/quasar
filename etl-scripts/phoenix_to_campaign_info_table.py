import MySQLdb
import config
import time
import sys
from DSPhoenixWebScraper import PhoenixScraper

phoenix_fetcher = PhoenixScraper()

total_pages = phoenix_fetcher.getPages()

page_number = 1

phoenix_campaigns = phoenix_fetcher.getCampaigns()

runs = {}

while page_number <= total_pages:
    phoenix_campaigns = phoenix_fetcher.getCampaigns(page_number)
    for i in phoenix_campaigns:
        id = i['id']
        runs[id] = []
        for j in i['campaign_runs']:
            for lang in i['campaign_runs'][j]:
                run_id = i['campaign_runs'][j][lang]['id']
                if (runs[id].count(run_id) == 0):
                    runs[id].append(run_id)
                print("Current runs are:", run_id)
    page_number += 1

print(runs)
