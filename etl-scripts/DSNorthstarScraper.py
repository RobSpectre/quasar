import json
import config
import oauthlib
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from QuasarWebScraper import Scraper

class NorthstarScraper(Scraper):

    # Define OAuth2 Client ID and Client Secret.
    ns_client = config.ns_client_id
    ns_secret = config.ns_client_secret

    # Create Client Object and Obtain Token
    client = BackendApplicationClient(client_id=ns_client)
    oauth = OAuth2Session(client=client)

    def __init__(self, ns_url='https://profile.dosomething.org'):
        Scraper.__init__(self, ns_url)

    # Send OAuth request for new or refresh token.
    # Note to self, should update this method to check for validity of token, and only refresh when necessary.
    # Something like if simple get().status = 422, then new_token, else, return existing token.
    def getToken(self, path='/v2/auth/token'):
        new_token = self.oauth.fetch_token(self.url + path, client_id=self.ns_client, client_secret=self.ns_secret, scope='admin')
        return new_token['access_token']

    # Override "get" method from QuasarWebScraper to include OAuth 2 token for authorization.
    def get(self, path, query_params=''):
        auth_headers = {'Authorization': 'Bearer ' + str(self.getToken())}
        response = self.session.get(self.url + path, headers=auth_headers, params=query_params)
        return response.json()

    # Get all users with max Nortstar user page. Default to first page of results.
    def getUsers(self, users=100, page_number=1):
        user_response = self.get('/v1/users', {'limit': users, 'page': page_number})
        return(user_response['data'])

    # Get total user count and pages. Returns array with member_count and total_pages.
    def userCount(self, users=100, page_number=1):
        get_count = self.get('/v1/users', {'limit': users, 'page': page_number})
        return(get_count['meta']['pagination']['total'], get_count['meta']['pagination']['total_pages'])
