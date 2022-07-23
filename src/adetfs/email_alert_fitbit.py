"""Module to send an email alert when fatal error has occurred
with Fitbit to REDCap automation program.

Requirements:
Gmail account (create one just for this)
Gmail application password (https://support.google.com/mail/answer/185833?hl=en)
properties.ini: Property file that has the email address and password of the sender
                and email address of the receiver
                properties.ini layout example can be found from Readme.md file

Returns:
None, except if connection failure -> error message in execute.log
"""


import configparser

from datetime import date

from email.message import EmailMessage

import smtplib

#https://towardsdatascience.com/send-data-alert-with-python-smtp-efb9ee08077e

#TODO: Change in other files so that token file path is not visible but
#instead we use config file and inside [filepath]
#TODO: Change the code to return the text so that if the connection
#fails we will know it and have it in the logs

#email properties
class EmailAlert:
    #Fetch following from a file
    config = configparser.ConfigParser()
    config.read('properties.ini')
    gmail_user = config['EMAIL']['user']
    gmail_password = config['EMAIL']['password']
    to = config['EMAIL']['to']

    def __init__(self,message):
        self.message = message

    def send_email(self):
        logf = open("execute.log", "a")
        msg = EmailMessage()
        msg.set_content('Fitbit Data Extraction weekly report. If any issues see below\n\n {}'.format(self.message))

        msg['Subject'] = 'Fitbit Data Extraction Alert {}'.format(str(date.today()))
        msg['From'] = self.gmail_user
        msg['To'] = self.to

        #email send request
        try:
            # Send the message via our own SMTP server.
            server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
            server.ehlo()
            server.login(self.gmail_user, self.gmail_password)
            server.send_message(msg)
            server.quit()
            server.close()
            #TODO: Remove print command when ready
            print ('Email sent!')
        except Exception as e:
            logf.write(f"{date.today().strftime('%Y_%m_%d')} Email failed : {e}\n")
            #TODO: Remove print command when ready
            print(e)
            print ('Something went wrong...')

        finally:
            logf.close()

#EmailAlert('test error').send_confirm_email()