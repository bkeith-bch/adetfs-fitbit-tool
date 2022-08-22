# ADETfs - Automated Data Extraction Tool for Fitbit server

Python software for automated extraction of user data from Fitbit server and saving it as csv. Software also saves all the sleep data as JSON file(s) which can be used for example for sleep graphics. This software can also be combined with import_to_redcap module, which can be used to transfer the data to REDCap.

## Installation

pip install adetfs

## Requirements

See the requirements.txt

## Usage

Software is easy-to-use but there are few tricks you must do.

This software is mainly intented to use in a research purposes but can also be used and modified to other purposes (see the license file). This software allows easy way to automate the data extraction from the Fitbit server. Fitbit server is not "open-source database" so you can only use this software with user(s) content.

This software can be run from the command-prompt using: python -m adetfs
Before launching, navigate to the folder that contains properties.ini file

It is also the responsibility of the person who uses this software to follow the local regulations according to any data protection and privacy laws.

## Properties.ini: This file contains all the parameters for the software to run. You only have to change the values of the parameters to match yours. Use only plain text, no quotations are needed. This file need to placed in the ADETfs folder path. Example of properties.ini file is available with the source code. Parameters in the file are:

#### EMAIL:

user = senders gmail address without '@gmail.com' (We suggest creating one for this purpose).  
password = gmail app password (https://support.google.com/accounts/answer/185833?hl=en)  
to = email address of receiver
usernames = filepath to txt file that contains json type dictionary with Fitbit userid and the name user want to use in email alert
Example: {"1XX23": "Test user 1", "9BB34":"Test user 2", "8CC35": "userid"}

#### CR:

id = client_id (can be obtained from Fitbit by registering an application)  
secret = client_secret (can be obtained from Fitbit by registering an application)

#### TOKENS:

token_file = path to token file. Create a file "filename.txt" and provide the path here.  Then use fetch_tokens_to_file module for saving the tokens inside the file.

#### REFRESH_TOKEN:

url_path = Fitbit API Refresh token URL path

#### SLEEP_STATS:

api_version = Fitbit API version for sleep stats

#### FOLDER_PATH

folder_path = Folder path in which the data will be saved.  Software will create a structure '\data\user_id\' in which the user files will be saved
Execute.log and data.log are save under this path

#### EXTRACTION_LOG:

EXTRACTION_LOG_PATH: Path to the extraction log file that contains JSON type information:  {USER_ID:last_extraction_date,USER_ID2:last_extraction_date}
This file is used to see if there is new data available for the user.
Extraction is being done until two days before last sync time to not miss any sleep data

### Complementary part for REDCap projects

#### REDCap:

token = REDCap project token  
url = REDCap project URL

### activate_main.bat: You can download this file from the project homepage

This file is for automation of the script. Automation will use the time delta between last extraction and last synctime. As a default the tool is meant to be run once a week. If run more seldom, make sure that all the data is being collected (Fitbit has rate limit of 150 queries per hour).

In this Batch file, you will need to add the directory of your Python virtualenvironment (if any), your python.exe file for that environment and the directory of this ADETfs main file.

You can use Windows Task Scheduler to schedule this file to be run once a week. At the beginning the file has simple check to verify that internet connection is on. If not it will sleep 10 minutes as a default before trying again. Loop will run 5 times and if no connection is established the execution will end and report can be find from the error.log.

### Client ID and Client Secret:

NEVER EXPOSE THE CLIENT ID AND SECRET!

Client refers to Fitbit Application you will have to create. Your application will have to be server type and follow the requirements and rules of Fitbit. The author(s) of this software, according to the license, are not in any legal responsibility for your use case.

Your application can use this tool as it is or make changes in accordance with the license.

When you have created and registered your App, you will have to save the credentials (client id and secret) into the corresponding parameters of Properties.ini file for later use.

Be aware, that if ever you will have to revoke the client secret, you will have to save the new secret inside the properties.ini.

### Tokens for each user:

NEVER EXPOSE THE USER TOKENS!

You must save your user(s) Fitbit id, expiration of authentication, access token and refresh token into the tokens text file. This have to be done manually using Fitbit Authentication. For this you can use fetch_tokens_to_file module. This module can be downloaded from the project homepage. There is also a copy of gather_keys_oauth2 module (original can be found from https://github.com/orcasgit/python-fitbit) After running the fetch_tokens_to_file module, a browser will open to Fitbit login page and ask the user to login. After login user will be asked the consent. After confirmation, the tokens will be saved in the tokens text file. Before fetching tokens for the next user, you will have to open Fitbit website and log out with the current user. Otherwise you will automatically fetch new tokens for the logged in user.

This part is most time consuming as it can not be automated. After doing this for each user, you should have a token file with each row having:
user_id,expires_at,access_token,refresh_token

Make sure that the file does not contain empty line at the end!

### CSV and JSON output:

Software will save one CSV file for each day for each user with the extracted data. Non-existing data will be empty. In the case of sleep data, if any, then software will save also JSON file for each day for each user. Each CSV file will be named as 'userid_YYYY_MM_DD_all_data.csv' and JSON file as 'sleep_stats_userid_YYYY_MM_DD.json'. If file is already existing name will contain '_copy' at the end. All files will be saved to folder called 'data\user_id'. Path for this folder can be configured inside properties.ini

### Error handling:

For fatal errors, software is using "execute.log" to save them. For non-fatal errors, for example non-existing sleep data, errors will be logged in "data_log.log".

### Automation:

Software comes with .bat-file which can be used for automation with most Windows OS. For Linux or other OS, please refer the OS help.

.bat-file, among other files like properties.ini, can be found from the source code or project website.

### Email alert:

Software is sending email alert after execution with information of possible errors and issues listed in the email.

### Tests:

Software includes few simple tests, which can be found from the tests folder. These can be used to check the correct functioning of the software.

## Contributing
Ideas how to improve the software, add new features or error fix are welcome.
Please open an issue first to discuss what you would like to change / or add.

## Citation
When used in research, please look the citation file and cite correspondingly.

## Acknowledgment
This tool is benefiting from the following projects:
Fitbit API Python Client Implementation , https://github.com/orcasgit/python-fitbit
CherryPy Object-Oriented HTTP framework, https://www.cherrypy.org

## License
[BSD 3-Clause](https://choosealicense.com/licenses/bsd-3-clause/)
This software includes third party open source software components: Pandas, CherryPy, Fitbit API Python Client Implementation , tqdm, Requests, urllib3, oauthlib, toml, and setuptools.

Each of these software components have their own license. Please see ./LICENSES/PANDAS_LICENSE, ./LICENSES/CHERRYPY_LICENSE, ./LICENSES/FITBITAPIPYTHONCLIENTIMPLEMENTATION_LICENSE, 
./LICENSES/TQDM_LICENSE, ./LICENSES/OAUTHLIB_LICENSE, ./LICENSES/SETUPTOOLS_LICENSE,
./LICENSES/URLLIB3_LICENSE, and ./LICENSES/REQUESTS_LICENSE., /LICENSES/TOML_LICENSE.