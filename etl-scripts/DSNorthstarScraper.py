import json
import config
import oauthlib
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from QuasarWebScraper import Scraper


# Setup new scraper class to Use Mailchimp endpoint and basic auth.
class NorthstarScraper(Scraper):

    def __init__(self):
        url = 'https://profile.dosomething.org'
        Scraper.__init__(self, url)

    # Define OAuth2 Client ID and Client Secret.
    ns_client = config.ns_client_id
    ns_secret = config.ns_client_secret

    # Create Client Object and Obtain Token
    client = BackendApplicationClient(client_id=ns_client)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(token_url='https://profile.dosomething.org/v2/auth/token', client_id=ns_client, client_secret=ns_secret, scope='admin')

    def showToken(self):
        print(self.token)

    # Define get for Mailchimp API calls.
    #def get(self, path, user=un, pass=pw, query_params=''):
#        response = self.session.get(self.url + path, auth=HTTPBasicAuth(un, pw), params=query_params)
        #return self.processResponse(response)

    # Parse response as JSON.
    #def processResponse(response):
#        parsed_response = response.json()
#        return parsed_response

ns_test_scraper = NorthstarScraper()
ns_test_scraper.showToken()
