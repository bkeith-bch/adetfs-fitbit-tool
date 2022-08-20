"""Program to export Fitbit data and save it as a csv file

Program includes also import_to_redcap module which can import
the extracted Fitbit data into REDCap project

Requires:
token.txt: file that has user_id,expires_in,access_token and refresh_token for each user whose data is being exported.
            One line per user and arguments separated by comma
cr.txt file that contains client_id and client_secret

Client_id and secret need to be saved in cr.txt file and they can be obtained by creating Fitbit Application
To obtain the user_id,expires_in, access_token and refresh_token you need to use fetch_tokens_to_file module

Returns:
csv file for each user with date of data export including the data from previous 7 days
execute.log that contains any error messages that have stopped the process
data_log.log that contains any error messages that have occurred during the process but which do not stop the program for running
"""
#Parts of the code / modules are using following packages:
#https://github.com/mGalarnyk/Python_Tutorials/blob/master/Apis/Fitbit/Fitbit_API.ipynb
#https://github.com/mGalarnyk/Python_Tutorials/blob/master/Apis/Fitbit/gather_keys_oauth2.py
#https://github.com/orcasgit/python-fitbit

#TODO: Use isort or manually sort imports to follow the common standard:
#alphabetically, then alphabetically from third party libraries
#and finally alphabetically from your own libraries
#when importing several from same library you can put them all with same import separated with comma
import configparser
import datetime as dt
import glob
import json
import os
import sys
import time
from datetime import date
from functools import reduce

import adetfs.clientsecret_and_usertokens as cliuser
import fitbit
import pandas as pd
import requests
import adetfs.update_tokens as update_tokens
from adetfs.activity_requests import ActivityStats
from adetfs.email_alert_fitbit import EmailAlert
from adetfs.sleep_stats_url import SleepStatsClass
from tqdm import tqdm

#TODO:Python licensing follow-up

length = cliuser.UserToken().length()
logf = open("execute.log", "a+")
data_logf = open("data_log.log", "a+")
redirect_uri='http://127.0.0.1:8080/'

#Following will fetch the application id and secret
CLIENT_ID = cliuser.ClientIdPwd().client()
CLIENT_SECRET = cliuser.ClientIdPwd().client()

#To count errors for email alert
error_counter = 0

#Successful URL connection codes
succesful_response = {200,201,204}

#Defining configparser to read properties.ini
config = configparser.ConfigParser()
config.read('properties.ini')

#End date is fixed to be the date two days before today

#Extraction log path
EXTRACTION_TIME_LOG_PATH = config['EXTRACTION_LOG']['EXTRACTION_LOG_PATH']

#Data folder path
FOLDER_PATH = config['FOLDER_PATH']['folder_path']

#Optional user names dictionary file path
USERNAMES = config['EMAIL']['usernames']

try:
    with open(f'{EXTRACTION_TIME_LOG_PATH}') as f:
        extraction_time_log = json.load(f)
except:
    extraction_time_log = {}

#Constant to use inside For-loop
TODAY = date.today()

#Function to run when rate limit has been reached
#Rate limit is reset on top of each hour
def rate_limit_reset(request_response):
    wait = int(request_response.headers['Fitbit-Rate-Limit-Reset'])+30 #Fitbit-Rate-Limit_Reset is in seconds
    print('Rate limit will be soon reached. Waiting until limit is reset')
    for seconds in tqdm(range(wait)):
        time.sleep(1)

#List of users whose data has not been collected since minimum 7 days
#On average, Fitbit watches keep minute-by-minute data for maximum 7 days
#This includes especially the sleep data
user_list = []

#List of users whose data was not extracted
no_data_extracted_user_list = []

#List for user id's with fatal error (fatal error is logged in execute.log)
fatal_error_list = []

#For-loop to go through line by line the file where user id's and tokens are saved
#FIXME: Fix so that if there is empty line, the code will not break. So if there is empty line between two users
#we will continue with the one after the line, and if it is empty line at the end, we will finish
for i in range(length):
    try:
        #Fetch the user id and tokens and open authenticated connection
        def fetch_auth_args(i):
            USER_ID = cliuser.UserToken().user(i)[0]
            ACCESS_TOKEN = cliuser.UserToken().user(i)[1]
            REFRESH_TOKEN = cliuser.UserToken().user(i)[2]
            EXPIRES_AT = cliuser.UserToken().user(i)[3]
            return USER_ID,ACCESS_TOKEN,REFRESH_TOKEN,EXPIRES_AT
        
        USER_ID,ACCESS_TOKEN,REFRESH_TOKEN,EXPIRES_AT = fetch_auth_args(i)

        #Check if data folder exist, if not create
        folder = f'{FOLDER_PATH}data/{USER_ID}'
        user_folder = glob.glob(folder)
        if not user_folder:  
            os.makedirs(folder)

        #Create connection object
        auth2_client = fitbit.Fitbit(CLIENT_ID,CLIENT_SECRET,oauth2=True,access_token=ACCESS_TOKEN,refresh_token=REFRESH_TOKEN,redirect_uri=redirect_uri) #expires_at=31536000,redirect_uri=redirect_uri,refresh_cb=

        #Verifying if the authorization was succesfull if not refreshing the tokens
        url_user_devices = "{0}/{1}/user/{user_id}/devices.json".format(
                    *fitbit.Fitbit._get_common_args(self=fitbit.Fitbit,user_id=USER_ID),
                    user_id=USER_ID
                )
        
        header = { 'Authorization': 'Bearer ' + ACCESS_TOKEN}
        verification_request = requests.post(url=url_user_devices,headers=header)
        response_code = verification_request.status_code

        def lastsynctime(request_text):
            try:
                LASTSYNCTIME = pd.to_datetime(json.loads(request_text.text)[0]['lastSyncTime'])
            except:
                LASTSYNCTIME = None
            finally:
                return LASTSYNCTIME

        
        try:
            LAST_EXTRACTION_TIME = pd.to_datetime(extraction_time_log[f'{USER_ID}'],format="%Y_%m_%d")
            
        except:          
            extraction_time_log[f'{USER_ID}'] = TODAY.strftime('%Y_%m_%d')
            with open(f'{EXTRACTION_TIME_LOG_PATH}', 'w') as json_file:
                json.dump(extraction_time_log, json_file)
            LAST_EXTRACTION_TIME = pd.to_datetime(TODAY.strftime('%Y_%m_%d'),format="%Y_%m_%d")

        #If succesfull
        #To check that we will not reach the rate limit and that we have
        #new data available
        #TODO: Change the rate limit from 50 to perhaps 30?
        
        if response_code in succesful_response:
            LASTSYNCTIME = lastsynctime(verification_request)
            if ((LASTSYNCTIME != None) and (LASTSYNCTIME.date() > LAST_EXTRACTION_TIME.date())):
                if int(verification_request.headers["Fitbit-Rate-Limit-Remaining"]) < 50:
                    rate_limit_reset(verification_request)
                else:
                    #user_list.append(USER_ID)
                    pass
            else:    
                #Continue seems to work as it will take us back to beginning of first for loop
                data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} No new data to extract for user {USER_ID}\n")
                print(f'No new data to extract for user {USER_ID}')
                #TODO: Check that following works and sends on the email all the users
                #whose data has not been collected since 14 days
                if (TODAY - LAST_EXTRACTION_TIME.date()).days > 7:
                    user_list.append(USER_ID)
                    continue
                else:
                    no_data_extracted_user_list.append(USER_ID)
                    continue            
            
        #If not succesfull
        #TODO: Add the following also in the data fetching parts?
        else:
            if response_code == 429:
                rate_limit_reset(verification_request)
                LASTSYNCTIME = lastsynctime(verification_request)
                if ((LASTSYNCTIME != None) and (LASTSYNCTIME.date() > LAST_EXTRACTION_TIME.date())):
                    pass
                else:
                    #TODO:have to implement this method so that if there is no new data we will
                    #not try to fetch anything.
                    #Continue seems to work as it will take us back to beginning of first for loop
                    if (TODAY - LAST_EXTRACTION_TIME.date()).days > 7:
                        user_list.append(USER_ID)
                        continue
                    else:
                        no_data_extracted_user_list.append(USER_ID)
                        continue
            elif response_code == 401:
                #FIXME: For some reason the code jumps to beginning of the main
                #loop after executing this part
                update_tokens.update_tokens(USER_ID,REFRESH_TOKEN,EXPIRES_AT)
                USER_ID,ACCESS_TOKEN,REFRESH_TOKEN,EXPIRES_AT = fetch_auth_args(i)
                #TODO:Remove print command when code is ready and functioning
                print(f'Fetching new tokens for user {USER_ID}')
                #print(USER_ID,ACCESS_TOKEN,REFRESH_TOKEN,EXPIRES_AT)
                auth2_client = fitbit.Fitbit(CLIENT_ID,CLIENT_SECRET,oauth2=True,access_token=ACCESS_TOKEN,refresh_token=REFRESH_TOKEN,redirect_uri=redirect_uri)
                header = { 'Authorization': 'Bearer ' + ACCESS_TOKEN}
                new_verification_request = requests.post(url=url_user_devices,headers=header)
                new_response_code = new_verification_request.status_code
                print(f'New response code {new_response_code}')
                #If succesfull
                if new_response_code in succesful_response:
                    LASTSYNCTIME = lastsynctime(new_verification_request)
                    if ((LASTSYNCTIME != None) and (LASTSYNCTIME.date() > LAST_EXTRACTION_TIME.date())):
                        pass
                    else:
                        if (TODAY - LAST_EXTRACTION_TIME.date()).days > 7:
                            user_list.append(USER_ID)
                            continue
                        else:
                            no_data_extracted_user_list.append(USER_ID)
                            continue
                else:
                    raise Exception('Failed after fetching new tokens: ',new_verification_request.text)
            else:
                raise Exception(new_verification_request.text)        

        #Define the range of data we want to fetch
        
        time_delta = LASTSYNCTIME.date()-LAST_EXTRACTION_TIME.date()
        print(f'Fetching data for {USER_ID}. Days passed {time_delta}')
        starttime = LASTSYNCTIME.date()-dt.timedelta(days=int(time_delta.days))
        #Endtime has been changed to be 2 days before last synctime to make sure
        #that data will be collected for all days. It is possible that patient
        #has synced the watch just after midnight and this way we could miss
        #the sleep data if using 1 day before
        endtime = LASTSYNCTIME.date()-dt.timedelta(days=2)
        #print(endtime)

        #Create empty lists that are needed for saving the data
        joined_list = []
        date_list = []
        df_list = []

        #Date range
        alldays = pd.date_range(start=starttime, end=endtime)
        
        #For loop to fetch data day by day for the date range
        for oneday in alldays:
            df_list = []
            oneday_str = oneday.date().strftime("%Y-%m-%d")
            oneday_str_filename = oneday.date().strftime("%Y_%m_%d")
            date_list.append(oneday_str)

            #To check at the beginning of every iteration if we are going to reach the api limit
            #By making the request here we avoid adding it under each API request
            verification_request_weekly = requests.post(url=url_user_devices,headers=header)
            if int(verification_request_weekly.headers["Fitbit-Rate-Limit-Remaining"]) < 30:
                rate_limit_reset(verification_request_weekly)
            
            #Request for step data
            try:
                oneday_step_data = auth2_client.time_series(resource=r'activities/steps',user_id=USER_ID, base_date=oneday,period='1d')
                df_step_data = pd.DataFrame(oneday_step_data['activities-steps'])
                df_step_data = df_step_data.rename(columns={'dateTime':'dateTimeStep','value':'step'})
                df_step_data.loc[:, 'date'] = pd.to_datetime(oneday)
                df_step_data.loc[:, 'user_id'] = USER_ID
            except Exception as e:
                data_logf.write(f"{date.today().strftime('%Y_%m_%d')} Step Data Failed for user {USER_ID}: {e}\n")
                data = [None,None]
                df_step_data = pd.DataFrame([data], columns=['dateTimeStep','step'])
                df_step_data.loc[:, 'date'] = pd.to_datetime(oneday)
                df_step_data.loc[:, 'user_id'] = USER_ID
            finally:
                #If Exception is "Too many Requests" then process will sleep until Fitbit limit reset +30 seconds have passed
                df_step_data.drop('dateTimeStep',axis=1,inplace=True)
                #FIXME: And same for other parts when "Too many request" need to be handled
                #At the moment if we reach request during the fetching we might miss the data for that day for that request
                if Exception == 'Too many Requests':
                    rate_limit_reset()
                    df_list.append(df_step_data)
                else:
                    df_list.append(df_step_data)
            
            #Request for distance data. Distance data is in US miles (1 mile = 1.609344 km)
            try:
                oneday_distance_data = auth2_client.time_series(resource=r'activities/distance',user_id=USER_ID, base_date=oneday,period='1d')
                df_distance_data = pd.DataFrame(oneday_distance_data['activities-distance'])
                df_distance_data = df_distance_data.rename(columns={'dateTime':'dateTimeDistance','value':'distance'})
                df_distance_data.loc[:, 'date'] = pd.to_datetime(oneday)
            except Exception as e:
                data_logf.write(f"{date.today().strftime('%Y_%m_%d')} Distance Data Failed for user {USER_ID}: {e}\n")
                data = [None,None]
                df_distance_data = pd.DataFrame([data], columns=['dateTimeDistance','distance'])
                df_distance_data.loc[:, 'date'] = pd.to_datetime(oneday)
            finally:
                df_distance_data.drop(['dateTimeDistance'],axis=1,inplace=True)
                if Exception == 'Too many Requests':
                    rate_limit_reset()
                else:
                    df_list.append(df_distance_data)
            
            #Request for sleep data
            try:
                url_sleep_stats = SleepStatsClass.sleep_stats_url(USER_ID,oneday)
                sleep_stats = auth2_client.make_request(url_sleep_stats)['sleep']
                for data in sleep_stats:
                    #There is no possibility to gather either classic or stages data so hear we specifically
                    #Make sure to gather the stages (classic is when no heart rate data is available)           
                    if data['isMainSleep'] == True and data['type'] == 'stages':
                        #print('Is mainsleep data')
                        #print(data)

                        #To catch the first sleep cycles for each sleep type and the details of the first "non wake" cycle
                        sleep_first_cycle = next(item for item in data['levels']['data'] if not item["level"] == "wake")
                        sleep_first_light = next(item for item in data['levels']['data'] if item["level"] == "light")
                        sleep_first_deep = next(item for item in data['levels']['data'] if item["level"] == "deep")
                        sleep_first_rem = next(item for item in data['levels']['data'] if item["level"] == "rem")

                        sleep_summary_df = pd.DataFrame({'sleep_cycle_start':[data['startTime']],
                        'sleep_cycle_end':[data['endTime']],
                        'first_cycle_start':sleep_first_cycle["dateTime"],
                        'first_cycle_level':sleep_first_cycle["level"],
                        'first_cycle_length_in_seconds':sleep_first_cycle['seconds'],
                        'first_light_sleep_start':sleep_first_light['dateTime'],
                        'first_deep_sleep_start':sleep_first_deep['dateTime'],
                        'first_rem_sleep_start':sleep_first_rem['dateTime'],
                        'minutes_of_sleep':[data['minutesAsleep']],
                        'minutes_awake':[data['minutesAwake']],
                        'number_of_awakenings':[data['levels']['summary']['wake']['count']],
                        'time_in_the_bed':[data['timeInBed']],
                        'minutes_sleep_rem':[data['levels']['summary']['rem']['minutes']],
                        'minutes_sleep_light':[data['levels']['summary']['light']['minutes']],
                        'minutes_sleep_deep':[data['levels']['summary']['deep']['minutes']],
                        'first_restless':None,
                        'first_awake':None,
                        'minutes_to_fall_asleep':None,
                        'minutes_after_wakeup':None,
                        'minutes_sleep_awake':None,
                        'minutes_sleep_restless':None,
                        'minutes_asleep':None})
                        sleep_summary_df.loc[:, 'date'] = pd.to_datetime(oneday)
                        #print('sleep summary df',sleep_summary_df)

                    elif data['isMainSleep'] == True and data['type'] == 'classic':
                        #To catch the first sleep cycles in the classic data
                        sleep_first_cycle = next(item for item in data['levels']['data'] if item["level"] == "asleep")
                        sleep_first_restless = next(item for item in data['levels']['data'] if item["level"] == "restless")
                        sleep_first_awake = next(item for item in data['levels']['data'] if item["level"] == "awake")

                        sleep_summary_df = pd.DataFrame({'sleep_cycle_start':[data['startTime']],
                        'sleep_cycle_end':[data['endTime']],
                        'first_cycle_start':sleep_first_cycle["dateTime"],
                        'first_cycle_level':sleep_first_cycle["level"],
                        'first_cycle_length_in_seconds':sleep_first_cycle['seconds'],
                        'first_light_sleep_start':None,
                        'first_deep_sleep_start':None,
                        'first_rem_sleep_start':None,
                        'minutes_of_sleep':[data['minutesAsleep']],
                        'minutes_awake':[data['minutesAwake']],
                        'number_of_awakenings':[data['levels']['summary']['awake']['count']],
                        'time_in_the_bed':[data['timeInBed']],
                        'minutes_sleep_rem':None,
                        'minutes_sleep_light':None,
                        'minutes_sleep_deep':None,
                        'first_restless':sleep_first_restless['dateTime'],
                        'first_awake':sleep_first_awake['dateTime'],
                        'minutes_to_fall_asleep':[data['minutesToFallAsleep']],
                        'minutes_after_wakeup':[data['minutesAfterWakeup']],
                        'minutes_sleep_awake':[data['levels']['summary']['awake']['minutes']],
                        'minutes_sleep_restless':[data['levels']['summary']['restless']['minutes']],
                        'minutes_asleep':[data['levels']['summary']['asleep']['minutes']]})
                        sleep_summary_df.loc[:, 'date'] = pd.to_datetime(oneday)
                        #print('sleep summary df',sleep_summary_df)
                    
                    else:
                        continue
                
                try:
                    if 'sleep_summary_df' in globals():
                        if sleep_summary_df.empty == False:
                            pass
                    
                        else:
                            data = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
                            sleep_summary_df = pd.DataFrame([data], columns=['sleep_cycle_start',
                                'sleep_cycle_end',
                                'first_cycle_start',
                                'first_cycle_level',
                                'first_cycle_length_in_seconds',
                                'first_light_sleep_start',
                                'first_deep_sleep_start',
                                'first_rem_sleep_start',
                                'minutes_of_sleep',
                                'minutes_awake',
                                'number_of_awakenings',
                                'time_in_the_bed',
                                'minutes_sleep_rem',
                                'minutes_sleep_light',
                                'minutes_sleep_deep',
                                'first_restless',
                                'first_awake',
                                'minutes_to_fall_asleep',
                                'minutes_after_wakeup',
                                'minutes_sleep_awake',
                                'minutes_sleep_restless',
                                'minutes_asleep'])
                            sleep_summary_df.loc[:, 'date'] = pd.to_datetime(oneday)

                    else:
                        data = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
                        sleep_summary_df = pd.DataFrame([data], columns=['sleep_cycle_start',
                            'sleep_cycle_end',
                            'first_cycle_start',
                            'first_cycle_level',
                            'first_cycle_length_in_seconds',
                            'first_light_sleep_start',
                            'first_deep_sleep_start',
                            'first_rem_sleep_start',
                            'minutes_of_sleep',
                            'minutes_awake',
                            'number_of_awakenings',
                            'time_in_the_bed',
                            'minutes_sleep_rem',
                            'minutes_sleep_light',
                            'minutes_sleep_deep',
                            'first_restless',
                            'first_awake',
                            'minutes_to_fall_asleep',
                            'minutes_after_wakeup',
                            'minutes_sleep_awake',
                            'minutes_sleep_restless',
                            'minutes_asleep'])
                        sleep_summary_df.loc[:, 'date'] = pd.to_datetime(oneday)
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    #print(e, exc_tb.tb_lineno)

                
                with open(f'{folder}/sleep_stats_{USER_ID}_{oneday_str_filename}.json', 'w+') as json_file:
                    json.dump(sleep_stats, json_file)
            except Exception as e:
                data_logf.write(f"{date.today().strftime('%Y_%m_%d')} Sleep Data Failed for user {USER_ID}: {e}\n")
                data = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
                sleep_summary_df = pd.DataFrame([data], columns=['sleep_cycle_start',
                            'sleep_cycle_end',
                            'first_cycle_start',
                            'first_cycle_level',
                            'first_cycle_length_in_seconds',
                            'first_light_sleep_start',
                            'first_deep_sleep_start',
                            'first_rem_sleep_start',
                            'minutes_of_sleep',
                            'minutes_awake',
                            'number_of_awakenings',
                            'time_in_the_bed',
                            'minutes_sleep_rem',
                            'minutes_sleep_light',
                            'minutes_sleep_deep',
                            'first_restless',
                            'first_awake',
                            'minutes_to_fall_asleep',
                            'minutes_after_wakeup',
                            'minutes_sleep_awake',
                            'minutes_sleep_restless',
                            'minutes_asleep'])
                sleep_summary_df.loc[:, 'date'] = pd.to_datetime(oneday)
            finally:
                #TODO: exception is perhaps not needed as this is handled at the beginning of the loop
                
                if Exception == 'Too many Requests':
                    rate_limit_reset()
                    df_list.append(sleep_summary_df)
                    
                else:
                    df_list.append(sleep_summary_df)
                    

            #Requests for activity minutes

            #Sedentary minutes
            try:
                sedentary_minutes_df = ActivityStats.sedentary_minutes(USER_ID,oneday,auth2_client)
            
            except Exception as e:
                data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Sedentary Activity Data Failed for user {USER_ID}: {e}\n")
            finally:
                if not isinstance(sedentary_minutes_df,pd.DataFrame):
                    #TODO: Check that this part works for all activity minutes
                    data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Sedentary Activity Data Failed for user {USER_ID}: {e}\n")
                    data = [None,None]
                    df_sedentary_data = pd.DataFrame([data], columns=['dateTimeminutesSedentary','minutessedentary'])
                    df_sedentary_data.loc[:, 'date'] = pd.to_datetime(oneday)
                    df_sedentary_data.drop('dateTimeminutesSedentary',axis=1,inplace=True)
                    df_list.append(df_sedentary_data)
                else:
                    df_list.append(sedentary_minutes_df)

            #Lightly active minutes
            try:
                light_minutes_df = ActivityStats.light_minutes(USER_ID,oneday,auth2_client)
            except Exception as e:
                data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Light Activity Data Failed for user {USER_ID}: {e}\n")
            finally:
                if not isinstance(light_minutes_df,pd.DataFrame):
                    data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Light Activity Data Failed for user {USER_ID}: {e}\n")
                    data = [None,None]
                    df_light_data = pd.DataFrame([data], columns=['dateTimeminutesLightlyActive','minuteslightlyactive'])
                    df_light_data.loc[:, 'date'] = pd.to_datetime(oneday)
                    df_light_data.drop('dateTimeminutesLightlyActive',axis=1,inplace=True)
                    df_list.append(df_light_data)
                else:
                    df_list.append(light_minutes_df)

            #Fairly active minutes
            try:
                fairly_minutes_df = ActivityStats.fairly_minutes(USER_ID,oneday,auth2_client)
            except Exception as e:
                data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Fairly Active Data Failed for user {USER_ID}: {e}\n")
            finally:
                if not isinstance(fairly_minutes_df,pd.DataFrame):
                    data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Fairly Active Data Failed for user {USER_ID}: {e}\n")
                    data = [None,None]
                    df_fair_data = pd.DataFrame([data], columns=['dateTimeminutesFairlyActive','minutesfairlyactive'])
                    df_fair_data.loc[:, 'date'] = pd.to_datetime(oneday)
                    df_fair_data.drop('dateTimeminutesFairlyActive',axis=1,inplace=True)
                    df_list.append(df_fair_data)

                else:
                    df_list.append(fairly_minutes_df)

            #Very active minutes
            try:
                very_active_minutes_df = ActivityStats.very_active_minutes(USER_ID,oneday,auth2_client)
            except Exception as e:
                data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Very Active Data Failed for user {USER_ID}: {e}\n")
            finally:
                if not isinstance(very_active_minutes_df,pd.DataFrame):
                    data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Very Active Data Failed for user {USER_ID}: {e}\n")
                    data = [None,None]
                    df_veryactive_data = pd.DataFrame([data], columns=['dateTimeminutesVeryActive','minutesveryactive'])
                    df_veryactive_data.loc[:, 'date'] = pd.to_datetime(oneday)
                    df_veryactive_data.drop('dateTimeminutesVeryActive',axis=1,inplace=True)
                    df_list.append(df_veryactive_data)

                else:
                    df_list.append(very_active_minutes_df)


            join_dfs_list = reduce(lambda df1,df2: df1.join(df2.set_index('date'),on='date'),df_list)
            joined_list.append(join_dfs_list)

        final_df_list = []
        final_df_list = joined_list
        final_dfs_list = [df.set_index('date') for df in final_df_list]

        #TODO:Add a method that will save the lastSyncTime as a new lastExtractionTime
        #when we have had succesful extraction!
        
        final_df = pd.concat(final_dfs_list, axis=0)
        #TODO:Following might be unnecessary?
        final_df.set_index(pd.to_datetime(final_df.index, format='%Y-%m-%d'))
        
        filename = f'{USER_ID}_{starttime.strftime("%Y_%m_%d")}_{endtime.strftime("%Y_%m_%d")}'
        writepath = os.path.join(folder,filename+'.csv')
        local_files = glob.glob(writepath)

        extraction_time_log[f'{USER_ID}'] = LASTSYNCTIME.strftime("%Y_%m_%d")

        with open(f'{EXTRACTION_TIME_LOG_PATH}','w') as jsonFile:
            json.dump(extraction_time_log, jsonFile)
        
        if not local_files:
            final_df.to_csv(writepath, index = 'date')
        else:
            logf.write("WARNING: This file {0} already exists! \
Filename changed to {1}\n".format(str(filename),str(filename+'_copy')))
            new_writepath = os.path.join(folder,filename+'_copy.csv')
            final_df.to_csv(new_writepath, index = 'date')

    except Exception as FatalError:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logf.write(f"{TODAY.strftime('%Y_%m_%d')} Failed to download {USER_ID}: {FatalError} : Line {exc_tb.tb_lineno}\n")
        error_counter += 1
        fatal_error_list.append(USER_ID)
        #Following is for testing if problems
        #exc_type, exc_obj, exc_tb = sys.exc_info()
        #fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        #print(exc_type, fname, exc_tb.tb_lineno)

#This part is to be only used if need to map another names for email
def user_name_mapping(user_list):
    with open(f'{USERNAMES}') as f:
        dictionary_data = f.read()
    usernames_dictionary = json.loads(dictionary_data)
    mapped_user_names = [usernames_dictionary[x] for x in user_list]
    return mapped_user_names

print(user_list)
if USERNAMES != None:
    new_user_list = user_name_mapping(user_list)
    new_no_data_extracted_user_list = user_name_mapping(no_data_extracted_user_list)
    user_list = new_user_list
    no_data_extracted_user_list = new_no_data_extracted_user_list
#print(new_user_list)
msg = EmailAlert(f"ADETfs has run successfully.\nEncountered {error_counter} errors \nFollowing \
users have not synced during the last 7 days or more\n\n{list(map(str, user_list))}\n\nFollowing users did not have new data for the past 7 days\n{list(map(str,no_data_extracted_user_list))}\n\nFollowing users data was not collected because fatal errors\n{list(map(str,fatal_error_list))}")
msg.send_email()
logf.close()
data_logf.close()