import configparser
import datetime as dt
from genericpath import isdir
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

"""
Wait for rate limit to reset
"""
def rate_limit_reset(request_response):
    wait = int(request_response.headers['Fitbit-Rate-Limit-Reset'])+30 #Fitbit-Rate-Limit_Reset is in seconds
    print('Rate limit will be soon reached. Waiting until limit is reset')
    for seconds in tqdm(range(wait)):
        time.sleep(1)

def fetch_auth_args(user):
    return user[0], user[1], user[2], user[3]

def main():
    num_users = cliuser.UserToken().length()
    redirect_uri='http://127.0.0.1:8080/'

    #Fitbit application credentials
    CLIENT_ID = cliuser.ClientIdPwd().client()
    CLIENT_SECRET = cliuser.ClientIdPwd().client()

    config = configparser.ConfigParser()
    config.read('properties.ini')

    EXTRACTION_TIME_LOG_PATH = config['EXTRACTION_LOG']['EXTRACTION_LOG_PATH']
    DATA_FOLDER_PATH = config['FOLDER_PATH']['folder_path']
    if DATA_FOLDER_PATH[-1] != '\\':
        DATA_FOLDER_PATH += '\\'

    # check data folder exists, create if not
    if not os.path.isdir(DATA_FOLDER_PATH):
        os.mkdir(DATA_FOLDER_PATH)
    
    logf = open(f"{DATA_FOLDER_PATH}\execute.log", "a+")
    data_logf = open(f"{DATA_FOLDER_PATH}\data_log.log", "a+")

    succesful_response_codes = {200,201,204}

    error_counter = 0
    fatal_error_list = []

    user_list = []
    no_data_extracted_user_list = []

    try:
        with open(f'{EXTRACTION_TIME_LOG_PATH}') as f:
            extraction_time_log = json.load(f)
    except:
        extraction_time_log = {}
    
    TODAY = date.today()
    YESTERDAY = TODAY-dt.timedelta(days=1)

    for ndx in range(num_users):
        try:
            if len(cliuser.UserToken().user(ndx)) == 4:
                USER_ID, ACCESS_TOKEN, REFRESH_TOKEN, EXPIRES_AT = fetch_auth_args(cliuser.UserToken().user(ndx))

                #Check if data folder exist, if not create
                folder = f'{DATA_FOLDER_PATH}data/{USER_ID}'
                user_folder = glob.glob(folder)
                if not user_folder:
                    os.makedirs(folder)

                #Create client connection object
                auth2_client = fitbit.Fitbit(CLIENT_ID,CLIENT_SECRET,oauth2=True,access_token=ACCESS_TOKEN,refresh_token=REFRESH_TOKEN,redirect_uri=redirect_uri)

                #verfiy if auth was successful
                url_user_devices = "{0}/{1}/user/{user_id}/devices.json".format(*fitbit.Fitbit._get_common_args(self=fitbit.Fitbit,user_id=USER_ID), user_id=USER_ID)
                header = { 'Authorization': 'Bearer ' + ACCESS_TOKEN}
                verification_request = requests.get(url=url_user_devices,headers=header)
                response_code = verification_request.status_code

                if response_code not in succesful_response_codes:
                    if response_code == 429: #too many requests
                        rate_limit_reset(verification_request)
                    elif response_code == 401: #unauthorized, try updating token
                        update_tokens.update_tokens(USER_ID, REFRESH_TOKEN, EXPIRES_AT)
                        USER_ID, ACCESS_TOKEN, REFRESH_TOKEN, EXPIRES_AT = fetch_auth_args(cliuser.UserToken().user(ndx))
                        auth2_client = fitbit.Fitbit(CLIENT_ID,CLIENT_SECRET,oauth2=True,access_token=ACCESS_TOKEN,refresh_token=REFRESH_TOKEN,redirect_uri=redirect_uri)
                        header = { 'Authorization': 'Bearer ' + ACCESS_TOKEN}
                    else:
                        # not an error we can handle here
                        raise Exception('Error fetching account devices: (' + response_code + ') ' + verification_request.text)
                    
                    #try again to get devices
                    verification_request = requests.get(url=url_user_devices, headers=header)
                    if verification_request.status_code not in succesful_response_codes:
                        raise Exception('Second Attempt of get devices failed for ' + USER_ID + ': (' + verification_request.status_code + ') ' + verification_request.text)
                
                # authorization should be all set here
                # just get data for yesterday and call it good
                print(f'Fetching data for {USER_ID}')

                #check api limit
                if int(verification_request.headers["Fitbit-Rate-Limit-Remaining"]) < 30:
                    rate_limit_reset(verification_request)
                
                joined_list = []
                df_list = []
                data_filename = YESTERDAY.strftime("%Y_%m_%d")
                
                #Step Data
                try:
                    step_data = auth2_client.time_series(resource=r'activities/steps', user_id=USER_ID, base_date=YESTERDAY, period='1d')
                    df_step_data = pd.DataFrame(step_data['activities-steps'])
                    df_step_data = df_step_data.rename(columns={'dateTime': 'dateTimeStep', 'value': 'step'})
                    df_step_data.loc[:, 'date'] = pd.to_datetime(YESTERDAY)
                    df_step_data.loc[:, 'user_id'] = USER_ID
                except Exception as e:
                    data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Step Data failed for user {USER_ID}: {e}\n")
                    data = [None,None]
                    df_step_data = pd.DataFrame([data], columns=['dateTimeStep','step'])
                    df_step_data.loc[:, 'date'] = pd.to_datetime(YESTERDAY)
                    df_step_data.loc[:, 'user_id'] = USER_ID
                finally:
                    df_step_data.drop('dateTimeStep',axis=1,inplace=True)
                    df_list.append(df_step_data)

                #Distance data
                try:
                    distance_data = auth2_client.time_series(resource=r'activities/distance',user_id=USER_ID, base_date=YESTERDAY,period='1d')
                    df_distance_data = pd.DataFrame(distance_data['activities-distance'])
                    df_distance_data = df_distance_data.rename(columns={'dateTime':'dateTimeDistance','value':'distance'})
                    df_distance_data.loc[:, 'date'] = pd.to_datetime(YESTERDAY)
                except Exception as e:
                    data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Distance Data failed for user {USER_ID}: {e}\n")
                    data = [None,None]
                    df_distance_data = pd.DataFrame([data], columns=['dateTimeDistance','distance'])
                    df_distance_data.loc[:, 'date'] = pd.to_datetime(YESTERDAY)
                finally:
                    df_distance_data.drop(['dateTimeDistance'],axis=1,inplace=True)
                    df_list.append(df_distance_data)

                #Sleep Data
                try:
                    url_sleep_stats = SleepStatsClass.sleep_stats_url(USER_ID, YESTERDAY)
                    sleep_stats = auth2_client.make_request(url_sleep_stats)['sleep']
                    for data in sleep_stats:
                        #There is no possibility to gather either classic or stages data so hear we specifically
                        #Make sure to gather the stages (classic is when no heart rate data is available)  
                        if data['isMainSleep'] == True and data['type'] == 'stages':

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
                            sleep_summary_df.loc[:, 'date'] = pd.to_datetime(YESTERDAY)

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
                            sleep_summary_df.loc[:, 'date'] = pd.to_datetime(YESTERDAY)

                    try:
                        if 'sleep_summary_df' in locals():
                            #print('IN locals\n')
                            if sleep_summary_df.empty == False:
                                #print('Not empty')
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
                                sleep_summary_df.loc[:, 'date'] = pd.to_datetime(YESTERDAY)

                        else:
                            #print('Not in locals\n')
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
                            sleep_summary_df.loc[:, 'date'] = pd.to_datetime(YESTERDAY)
                    except Exception as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()

                    with open(f'{folder}/sleep_stats_{USER_ID}_{data_filename}.json', 'w+') as json_file:
                        json.dump(sleep_stats, json_file)
                
                except Exception as e:
                    data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Sleep Data Failed for user {USER_ID}: {e}\n")
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
                    sleep_summary_df.loc[:, 'date'] = pd.to_datetime(YESTERDAY)
                finally:
                    df_list.append(sleep_summary_df)

                #Sedentary minutes
                try:
                    sedentary_minutes_df = ActivityStats.sedentary_minutes(USER_ID,YESTERDAY,auth2_client)
                except Exception as e:
                    data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Sedentary Activity Data Failed for user {USER_ID}: {e}\n")
                finally:
                    if not isinstance(sedentary_minutes_df,pd.DataFrame):
                        data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Sedentary Activity Data Failed for user {USER_ID}: {e}\n")
                        data = [None,None]
                        df_sedentary_data = pd.DataFrame([data], columns=['dateTimeminutesSedentary','minutessedentary'])
                        df_sedentary_data.loc[:, 'date'] = pd.to_datetime(YESTERDAY)
                        df_sedentary_data.drop('dateTimeminutesSedentary',axis=1,inplace=True)
                        df_list.append(df_sedentary_data)
                    else:
                        df_list.append(sedentary_minutes_df)

                #Lightly Active minutes
                try:
                    light_minutes_df = ActivityStats.light_minutes(USER_ID,YESTERDAY,auth2_client)
                except Exception as e:
                    data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Light Activity Data Failed for user {USER_ID}: {e}\n")
                finally:
                    if not isinstance(light_minutes_df,pd.DataFrame):
                        data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Light Activity Data Failed for user {USER_ID}: {e}\n")
                        data = [None,None]
                        df_light_data = pd.DataFrame([data], columns=['dateTimeminutesLightlyActive','minuteslightlyactive'])
                        df_light_data.loc[:, 'date'] = pd.to_datetime(YESTERDAY)
                        df_light_data.drop('dateTimeminutesLightlyActive',axis=1,inplace=True)
                        df_list.append(df_light_data)
                    else:
                        df_list.append(light_minutes_df)

                #Fairly active minutes
                try:
                    fairly_minutes_df = ActivityStats.fairly_minutes(USER_ID,YESTERDAY,auth2_client)
                except Exception as e:
                    data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Fairly Active Data Failed for user {USER_ID}: {e}\n")
                finally:
                    if not isinstance(fairly_minutes_df,pd.DataFrame):
                        data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Fairly Active Data Failed for user {USER_ID}: {e}\n")
                        data = [None,None]
                        df_fair_data = pd.DataFrame([data], columns=['dateTimeminutesFairlyActive','minutesfairlyactive'])
                        df_fair_data.loc[:, 'date'] = pd.to_datetime(YESTERDAY)
                        df_fair_data.drop('dateTimeminutesFairlyActive',axis=1,inplace=True)
                        df_list.append(df_fair_data)

                    else:
                        df_list.append(fairly_minutes_df)

                #Very active minutes
                try:
                    very_active_minutes_df = ActivityStats.very_active_minutes(USER_ID,YESTERDAY,auth2_client)
                except Exception as e:
                    data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Very Active Data Failed for user {USER_ID}: {e}\n")
                finally:
                    if not isinstance(very_active_minutes_df,pd.DataFrame):
                        data_logf.write(f"{TODAY.strftime('%Y_%m_%d')} Very Active Data Failed for user {USER_ID}: {e}\n")
                        data = [None,None]
                        df_veryactive_data = pd.DataFrame([data], columns=['dateTimeminutesVeryActive','minutesveryactive'])
                        df_veryactive_data.loc[:, 'date'] = pd.to_datetime(YESTERDAY)
                        df_veryactive_data.drop('dateTimeminutesVeryActive',axis=1,inplace=True)
                        df_list.append(df_veryactive_data)

                    else:
                        df_list.append(very_active_minutes_df)

                joined_list = [reduce(lambda df1,df2: df1.join(df2.set_index('date'),on='date'),df_list)]
                # joined_list.set_index('date')
                final_dfs_list = [df.set_index('date') for df in joined_list]
                # final_df = joined_list
                final_df = pd.concat(final_dfs_list, axis=0)

                final_df.set_index(pd.to_datetime(final_df.index, format='%Y-%m-%d'))

                filename = f'{USER_ID}_{YESTERDAY.strftime("%Y_%m_%d")}'
                writepath = os.path.join(folder,filename+'.csv')
                local_files = glob.glob(writepath)

                if not local_files:
                    final_df.to_csv(writepath, index = 'date')
                else:
                    logf.write("WARNING: This file {0} already exists! \ Filename changed to {1}\n".format(str(filename),str(filename+'_copy')))
                    new_writepath = os.path.join(folder,filename+'_copy.csv')
                    final_df.to_csv(new_writepath, index = 'date')

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logf.write(f"{TODAY.strftime('%Y_%m_%d')} Error at {ndx} account: {e} : Line {exc_tb.tb_lineno}\n")

    logf.close()
    data_logf.close()

if __name__ == "__main__":
    main()