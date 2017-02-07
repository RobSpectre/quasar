import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

class Scraper:
    """Top-level web-scraper class for the DoSomething.org data platform, Quasar.

    The intent is to extend this to handle the various DS services as their own
    library for various services, e.g. Northstar, Gambit, Rogue, etc.

    The code below is re-purposed and inspired by
    http://stackoverflow.com/questions/21371809/cleanly-setting-max-retries-on-python-requests-get-or-post-method.

    This code was object orientified primarily by David Furnes @DFurnes to help
    me learn more about OOP in general and in Python specifically.
    """

    def __init__(self, url, retry_total=6, backoff_time=1.9):
        """Set up http/s requests with retries and backoff intervals.

        Args:
            url (str): URL to make request against.
            retry_total (int): Total number of retries.
            backoff_time (float): In-between backup intervals.
        """
        # Create a session
        self.session = requests.Session()
        self.url = url

        # Define retries and backoff time for http and https urls.
        http_retries = Retry(total=retry_total, backoff_factor=backoff_time)
        https_retries = Retry(total=retry_total, backoff_factor=backoff_time)

        # Create adapters with the retry logic for each.
        http = requests.adapters.HTTPAdapter(max_retries=http_retries)
        https = requests.adapters.HTTPAdapter(max_retries=https_retries)

        # Replace the session's original adapters.
        self.session.mount('http://', http)
        self.session.mount('https://', https)

    def get(self, path, query_params=''):
        """Basic GET request method.

        Args:
            path (str): Add to base URL defined by init for full URI.
            query_params (dict): Query parameters for a request, default none.
        """
        response = self.session.get(self.url + path, params=query_params)
        return self.processResponse(response)

    def post(self, path, body=[]):
        """Basic POST request method.

        Args:
            path (str): Add to base URL defined by init for full URI.
            body (dict): Post data for a request, default none.
        """
        response = self.session.post(self.url + path, body=body)
        return self.processResponse(response)

    def processResponse(response):
        """Stub method to handle data processing in vFuture."""
        return response
