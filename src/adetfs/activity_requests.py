"""Module to fetch the activity time data from Fitbit server

Parameters:
USER_ID: Unique Fitbit USER_ID
oneday: Day which the data is being collected

Returns:
Pandas Dataframe

"""

import pandas as pd


class ActivityStats():
    def sedentary_minutes(USER_ID,oneday,auth2_client):
        
        try:
            oneday_sedentary_data = auth2_client.time_series(resource=r'activities/minutesSedentary',user_id=USER_ID, base_date=oneday,period='1d')
            df_sedentary_data = pd.DataFrame(oneday_sedentary_data['activities-minutesSedentary'])
            df_sedentary_data = df_sedentary_data.rename(columns={'dateTime':'dateTimeminutesSedentary','value':'minutessedentary'})
            df_sedentary_data.loc[:, 'date'] = pd.to_datetime(oneday)
        except Exception as e:
            data = [None,None]
            df_sedentary_data = pd.DataFrame([data], columns=['dateTimeminutesSedentary','minutessedentary'])
            df_sedentary_data.loc[:, 'date'] = pd.to_datetime(oneday)
        finally:
            df_sedentary_data.drop('dateTimeminutesSedentary',axis=1,inplace=True)
            if Exception == 'Too many Requests':
                return 'Too many Requests'
            else:
                return df_sedentary_data

    def light_minutes(USER_ID,oneday,auth2_client):
        
        try:
            oneday_light_data = auth2_client.time_series(resource=r'activities/minutesLightlyActive',user_id=USER_ID, base_date=oneday,period='1d')
            df_light_data = pd.DataFrame(oneday_light_data['activities-minutesLightlyActive'])
            df_light_data = df_light_data.rename(columns={'dateTime':'dateTimeminutesLightlyActive','value':'minuteslightlyactive'})
            df_light_data.loc[:, 'date'] = pd.to_datetime(oneday)
        except Exception as e:
            data = [None,None]
            df_light_data = pd.DataFrame([data], columns=['dateTimeminutesLightlyActive','minuteslightlyactive'])
            df_light_data.loc[:, 'date'] = pd.to_datetime(oneday)
        finally:
            df_light_data.drop('dateTimeminutesLightlyActive',axis=1,inplace=True)
            if Exception == 'Too many Requests':
                return 'Too many Requests'
            else:
                return df_light_data

    def fairly_minutes(USER_ID,oneday,auth2_client):

        try:
            oneday_fairly_data = auth2_client.time_series(resource=r'activities/minutesFairlyActive',user_id=USER_ID, base_date=oneday,period='1d')
            df_fair_data = pd.DataFrame(oneday_fairly_data['activities-minutesFairlyActive'])
            df_fair_data = df_fair_data.rename(columns={'dateTime':'dateTimeminutesFairlyActive','value':'minutesfairlyactive'})
            df_fair_data.loc[:, 'date'] = pd.to_datetime(oneday)
        except Exception as e:
            data = [None,None]
            df_fair_data = pd.DataFrame([data], columns=['dateTimeminutesFairlyActive','minutesfairlyactive'])
            df_fair_data.loc[:, 'date'] = pd.to_datetime(oneday)
        finally:
            df_fair_data.drop('dateTimeminutesFairlyActive',axis=1,inplace=True)
            if Exception == 'Too many Requests':
                return 'Too many Requests'
            else:
                return df_fair_data

    def very_active_minutes(USER_ID,oneday,auth2_client):

        try:
            oneday_active_data = auth2_client.time_series(resource=r'activities/minutesVeryActive',user_id=USER_ID, base_date=oneday,period='1d')
            df_active_data = pd.DataFrame(oneday_active_data['activities-minutesVeryActive'])
            df_active_data = df_active_data.rename(columns={'dateTime':'dateTimeminutesVeryActive','value':'minutesveryactive'})
            df_active_data.loc[:, 'date'] = pd.to_datetime(oneday)
        except Exception as e:
            data = [None,None]
            df_active_data = pd.DataFrame([data], columns=['dateTimeminutesVeryActive','minutesveryactive'])
            df_active_data.loc[:, 'date'] = pd.to_datetime(oneday)
        finally:
            df_active_data.drop('dateTimeminutesVeryActive',axis=1,inplace=True)
            if Exception == 'Too many Requests':
                return 'Too many Requests'
            else:
                return df_active_data