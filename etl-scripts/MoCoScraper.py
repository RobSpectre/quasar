import config
from datetime import datetime, date, timedelta
from bs4 import BeautifulSoup
from QuasarWebScraper import Scraper
from requests.auth import HTTPBasicAuth


class MoCoScraper(Scraper):
    """Class for extracting Phoenix data via API."""

    # Basic auth credentials for Phoenix API.
    moco_user = config.mc_user
    moco_pass = config.mc_pw

    # Get current time based on object creation.
    now_iso = datetime.now().isoformat().replace("-", "").replace(":", "").split(".")
    now_iso = now_iso[0]

    def __init__(self, moco_url='https://secure.mcommons.com'):
        """Set MoCo API with all retry goodness of Quasar Web Scraper."""
        Scraper.__init__(self, moco_url)

    def get(self, path, query_params=''):
        """Set get method to include basic auth."""
        response = self.session.get(self.url + path,
                                    auth=HTTPBasicAuth(self.ds_user,
                                                       self.ds_pass),
                                    params=query_params)
        return response

    def getAllUsers(self, backfill_time, page_num = 1, per_page=1000):
        """Get all users from specified time, begin with page 1, 1K per page."""
        origin_time = now - timedelta(hours=int(backfill_time))
        origin_time_iso = origin_time.isoformat().replace("-", "").replace(":", "").split(".")
        origin_time_iso = origin_time_iso[0]
        users_req = self.get('/api/profiles', {'limit': per_page, 'page': page_num, 'from': origin_time_iso})
        users_parse = BeautifulSoup(users_req.text, 'xml')
        users = users_parse.find_all('profile')
        return users
