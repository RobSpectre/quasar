import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# The code below is re-purposed and inspired by
# http://stackoverflow.com/questions/21371809/cleanly-setting-max-retries-on-python-requests-get-or-post-method
# and this was object orientified primarily by David Furnes @DFurnes to help me learn
# more about OOP in general and via Python specifically.


class Scraper:

    # Set Default values for retry and backoff times.
    def __init__(self, url, retry_total=6, backoff_time=300):
        # Create a session
        self.session = requests.Session()
        self.url = url

        # Define retries and backoff time for http and https urls
        http_retries = Retry(total=retry_total, backoff_factor=backoff_time)
        https_retries = Retry(total=retry_total, backoff_factor=backoff_time)

        # Create adapters with the retry logic for each
        http = requests.adapters.HTTPAdapter(max_retries=http_retries)
        https = requests.adapters.HTTPAdapter(max_retries=https_retries)

        # Replace the session's original adapters
        self.session.mount('http://', http)
        self.session.mount('https://', https)

    # Define Basic Get/Post Logic
    def get(self, path, query_params=''):
        response = self.session.get(self.url + path, params=query_params)
        return self.processResponse(response)

    def post(self, path, body = []):
        response = self.session.post(self.url + path, body=body)
        return self.processResponse(response)

    # Stub method to handle data processing later
    def processResponse(response):
        return response
