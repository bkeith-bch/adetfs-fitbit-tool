"""Module for updating the tokens

Two functions can be called:
new_access_token: Function that saves new tokens to a file
update_tokens: Function that fetches new tokens

Requirements:
properties.ini: File with refresh token url path and path to token file
               Layout example of the file can be found from Readme.md

Parameters:
user_id: Fitbit user ID
refresh_token: Fitbit refresh token to fetch new tokens
expires_in: Token expire time

Returns:
None
"""

import requests
import re
import base64
import json
import configparser

config = configparser.ConfigParser()
config.read('properties.ini')
url = config['REFRESH_TOKEN']['url_path']
token_file_path = config['TOKENS']['token_file']
CLIENT_ID = config['CR']['id']
CLIENT_SECRET = config['CR']['secret']

def new_acces_token(USER_ID,token):
   new_lines = []
   with open (f"{token_file_path}",'r') as f:
      data = f.readlines()
        
   for line in data:
      substring = "{}.+".format(USER_ID)
      new_line = ("{0},{1},{2},{3}".format(USER_ID,token['expires_in'],token['access_token'],token['refresh_token']))
      line = re.sub(r'%s.+' % substring,new_line,line)
      new_lines.append(line)

   with open(f"{token_file_path}",'w') as file:
      file.writelines(new_lines)


def update_tokens(user_id,refresh_token,expires_at):

   USER_ID = user_id
   REFRESH_TOKEN = refresh_token

   #Fetching the client id and secret and encoding them with base64
   secret = CLIENT_ID + ":" + CLIENT_SECRET
   b64secret = base64.b64encode(secret.encode())
   b64decoded_option = b64secret.decode().strip()
   REFRESH_TOKEN_STR = str(REFRESH_TOKEN).rstrip()

   #Creating the request header and body
   header = { 'Authorization': 'Basic ' + b64secret.decode(),"Content-Type": "application/x-www-form-urlencoded"}
   data = {}
   data["grant_type"] = "refresh_token"
   data["refresh_token"] = REFRESH_TOKEN_STR

   #Requesting the new tokens
   req = requests.post(url,data=data,headers=header)
   
   #Print command can be used if there seems to be problem with saving the tokens
   #This print will print the new tokens in the terminal
   #print("New tokens: ",req.text)
   
   json_response = json.loads(req.text)
   
   #Next part will update tokens in the token file
   new_acces_token(USER_ID,json_response)
