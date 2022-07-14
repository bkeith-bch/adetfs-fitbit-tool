"""Module to fetch Fitbit sleep data

For using this module you have to have initialized authorization to Fitbit server

Requirements:
properties.ini: File that contains API version for the url
                Properties.ini layout example can be found from Readme.md

Parameters:
USER_ID: Unique Fitbit USER_ID
oneday: Day which the data is being collected

Returns:
URL string
"""

import configparser

import fitbit

config = configparser.ConfigParser()
config.read('properties.ini')
api_version = config['SLEEP_STATS']['api_version']
#For sleep we need to use date object

class SleepStatsClass():
    #Class method to return the daily sleep data url
    def sleep_stats_url(USER_ID,oneday):
        df_list = []
        url_sleep_stats = "{0}/{api}/user/{user_id}/sleep/date/{year}-{month}-{day}.json".format(
                    *fitbit.Fitbit._get_common_args(self=fitbit.Fitbit,user_id=USER_ID),
                    api=api_version,
                    user_id=USER_ID,
                    year=oneday.year,
                    month=oneday.month,
                    day=oneday.day
                )
        return url_sleep_stats