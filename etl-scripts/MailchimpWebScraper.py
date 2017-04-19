import json
import config
import time
from datetime import datetime
from QuasarWebScraper import Scraper
from requests.auth import HTTPBasicAuth


class PhoenixScraper(Scraper):
    """Class for extracting Phoenix data via API."""


    # MailChimp API v3.0 HTTP Simple Auth Credentials
    un = config.mailchimp_api_user
    pw = config.mailchimp_api_pass

    # Set start-teme as object creation time.
    time_now = time.time()

    def __init__(self, ds_phoenix_url='https://us4.api.mailchimp.com'):
        """Set Phoenix API with all retry goodness of Quasar Web Scraper."""
        Scraper.__init__(self, ds_phoenix_url)

    def get(self, path, query_params=''):
        """Set get method to include basic auth."""
        response = self.session.get(self.url + path,
                                    auth=HTTPBasicAuth(self.ds_user,
                                                       self.ds_pass),
                                    params=query_params)
        return response.json()

    def getSubscribedMembers(self, backfill_hours=4, offset=0):
        """Get members from Main DS List."""
        origin_time = time_now - (backfill_hours * 3600)
        info = {'status': 'subscribed',
                'since_timestamp_opt': datetime.utcfromtimestamp(origin_time).isoformat(),
                'before_timestamp_opt': datetime.utcfromtimestamp(time_now).isoformat(),
                'count': mc_increment, 'fields': 'members.email_address,members.timestamp_opt,members.status,members.stats,members.list_id,members.location',
                'offset': member_offset}
        self.get('')
    def getPages(self):
        """Get total campaign pages."""
        page_response = self.get('/api/v1/campaigns', {'page': 1})
        return(page_response['pagination']['total_pages'])

    def getCampaigns(self, page_number=1):
        """Get campaigns page by page from DS Phoenix API.

        Args:
            page_number (int): Page number to return, default 1.
        """
        user_response = self.get('/api/v1/campaigns', {'page': page_number})
        return(user_response['data'])
