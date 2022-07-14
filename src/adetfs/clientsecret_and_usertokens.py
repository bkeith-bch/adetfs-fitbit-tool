"""Module that returns arguments that are needed for fitbit_main program

Requires:
properties.ini: File that has the path for the token.txt and cr.txt files
                properties.ini layout example can be found from Readme.md file
token.txt:  File that has user_id,access_token and refresh_token for each user whose data is being exported.
            One line per user and arguments separated by comma
cr.txt:     File that contains client_id and client_secret on one line separated by comma

Program has two classes:

ClientIdPwd class returns client_id and client_secret
UserToken class returns user_id,access_token and refresh_token

Client_id and secret need to be saved in txt file and they can be obtained by creating Fitbit Application

Access and Refresh token can be obtained using module 'fetch_tokens_to_file'

Returns:
String
"""

import configparser

from typing import Tuple

#Class for fetching client credentials and number of clients from a file
class ClientIdPwd():
    config = configparser.ConfigParser()
    config.read('properties.ini')
    cr_id = config['CR']['id']
    cr_secret = config['CR']['secret']
    token_file_path = config['TOKENS']['token_file']

    #FIXME: Should we add @staticmethod for this?
    def __init__(self):
        self.token_file_path = self.token_file_path
        self.cr_id = self.cr_id
        self.cr_secret = self.cr_secret
        
    def client(self) -> Tuple[str,str]:
        return self.cr_id, self.cr_secret

#Class for fetching user tokens and number of users
class UserToken():
    config = configparser.ConfigParser()
    config.read('properties.ini')
    token_file_path = config['TOKENS']['token_file']

    def __init__(self):
        self.token_file_path = self.token_file_path

    def user(self,line:int) -> Tuple[str,str,str,str]:
        with open(f"{self.token_file_path}") as f:
            lines = f.readlines()
            USER_ID = lines[line].split(',')[0]
            EXPIRES_AT = lines[line].split(',')[1]
            ACCESS_TOKEN = lines[line].split(',')[2]
            REFRESH_TOKEN = lines[line].split(',')[3]
        return USER_ID,ACCESS_TOKEN,REFRESH_TOKEN,EXPIRES_AT

    def length(self) -> int:
        with open(f"{self.token_file_path}") as f:
            line_count = sum(1 for line in f if line.strip())
        return line_count