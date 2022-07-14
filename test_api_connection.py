"""This module is for testing the Connection to Fitbit API

This module will connect to the part of the API that does not
require user information
"""
import configparser

import fitbit

config = configparser.ConfigParser()
config.read('properties.ini')
cr_id = config['CR']['id']
cr_secret = config['CR']['secret']

unauth_client = fitbit.Fitbit(cr_id,cr_secret)
# certain methods do not require user keys
unauth_client.food_units()
#TODO:Add also another test module that will for example
#check that all the required files and properties
#(properties.ini) can be found