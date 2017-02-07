import json
import config
import oauthlib
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from QuasarWebScraper import Scraper

class NorthstarScraper(Scraper):

    def __init__(self):
        url = 'https://profile.dosomething.org/'
        Scraper.__init__(self, url)

    # Define OAuth2 Client ID and Client Secret.
    ns_client = config.ns_client_id
    ns_secret = config.ns_client_secret

    # Create Client Object and Obtain Token
    client = BackendApplicationClient(client_id=ns_client)
    oauth = OAuth2Session(client=client)
    # Stub initial token request to be used in getToken method improvement below.
    # token = oauth.fetch_token(token_url='https://profile.dosomething.org/v2/auth/token', client_id=ns_client, client_secret=ns_secret, scope='admin')

    # Send OAuth request for new or refresh token.
    # Not to self, should update this method to check for validity of initial token above, and only refresh when necessary.
    # Something like if simple get().status = 422, then new_token, else, return existing token.
    def getToken(self):
        new_token = self.oauth.fetch_token(token_url='https://profile.dosomething.org/v2/auth/token', client_id=self.ns_client, client_secret=self.ns_secret, scope='admin')
        return new_token['access_token']

    # Override "get" method from QuasarWebScraper to include OAuth 2 token for authorization.
    def get(self, path, query_params=''):
        auth_headers = {'Authorization': 'Bearer ' + str(self.getToken())}
        response = self.session.get(self.url + path, headers=auth_headers, params=query_params)
        return self.processResponse(response)

        # return response.json()

    # Stub class for parsing any response as JSON. Currently has some parsing issues.
    def processResponse(self, response):
        parsed_response = response.json()
        return parsed_response

    # Get all users with max Nortstar user page. Default to first page of results.
    def getUsers(self, users=100, page_number=1):
        user_response = self.get('/v1/users', {'limit': users, 'page': page_number})
        return(user_response['data'])

    # Get total user count. Using 1 page and 1 user to minimize request response time.
    def userCount(self, users=1, page_number=1):
        get_count = self.get('/v1/users', {'limit': users, 'page': page_number})
        return(get_count['meta']['pagination']['total'])

    # Get total pages based on getUsers default values (can be overridden).
    def pageCount(self, users=100, page_number=1):
        page_count = self.get('/v1/users', {'limit': users, 'page': page_number})
        return(page_count['meta']['pagination']['total_pages'])

testNS = NorthstarScraper()
print(testNS.userCount())
print(testNS.getUsers(2,2))
