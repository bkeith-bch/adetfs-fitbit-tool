from asyncio.windows_events import NULL
from datetime import datetime
import pandas as pd
import unittest
import configparser

class Test_TestFetchAuthArgs(unittest.TestCase):
    
    #To test that properties.ini file exists in the path
    def test_loadFile(self):
        self.config = configparser.ConfigParser()
        self.testfile = self.config.read('properties.ini')
    
    #To test that token file path is correctly implemented in properties.ini
    def test_fetch_auth_args(self):
        self.config = configparser.ConfigParser()
        self.config.read('properties.ini')
        self.test_token_path = self.config['TOKENS']['token_file']
        return self.test_token_path

    #To test that first token line in the tokens file is in correct format    
    def test_tokens(self):
        self.test_token_path = Test_TestFetchAuthArgs.test_fetch_auth_args(self)
        with open(f"{self.test_token_path}") as f:
            lines = f.readlines()
            self.USER_ID = lines[0].split(',')[0]
            self.EXPIRES_AT = lines[0].split(',')[1]
            self.ACCESS_TOKEN = lines[0].split(',')[2]
            self.REFRESH_TOKEN = lines[0].split(',')[3]
        self.assertTrue(type(self.USER_ID) is str and not NULL)
        self.assertTrue(type(self.EXPIRES_AT) is int or float and not NULL)
        self.assertTrue(type(self.ACCESS_TOKEN) is str and not NULL)
        self.assertTrue(type(self.REFRESH_TOKEN) is str and not NULL)
        self.assertTrue(self.REFRESH_TOKEN != self.ACCESS_TOKEN)

if __name__ == '__main__':
    unittest.main()