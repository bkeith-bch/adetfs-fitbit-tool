"""Module to save user id and tokens in a txt file

Fitbit client id and secret need to be obtained by creating
new Fitbit application (see Fitbit Register an App).

Module is using execute.log file for writing possible errors

Requires:
properties.ini: Property file that contains path to the token txt file
                and the client credentials as follows:
                client id: Fitbit client id
                client secret: Fitbit client secret
                Properties.ini layout example can be found from Readme.md

Returns:
Nothing 
"""

import configparser

import gather_keys_oauth2 as oauth2


logf = open("execute.log", "a")

config = configparser.ConfigParser()
config.read('properties.ini')
token_file_path = config['TOKENS']['token_file']
CLIENT_ID = config['CR']['id']
CLIENT_SECRET = config['CR']['secret']

try:
    #Connecting to server to obtain user consent and to get the tokens and user id
    server = oauth2.OAuth2Server(CLIENT_ID,CLIENT_SECRET)
    server.browser_authorize()

    ACCESS_TOKEN = str(server.fitbit.client.session.token['access_token'])
    REFRESH_TOKEN = str(server.fitbit.client.session.token['refresh_token'])
    USER_ID = str(server.fitbit.client.session.token['user_id'])
    EXPIRES_AT = str(server.fitbit.client.session.token['expires_at'])
         
    with open (f"{token_file_path}",'a') as f:
        line = ("{0},{1},{2},{3}\n".format(USER_ID,EXPIRES_AT,ACCESS_TOKEN,REFRESH_TOKEN))
        f.write(line)
except Exception as e:
    print(e)

logf.close()