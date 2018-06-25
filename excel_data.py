'''
Created on Jun 11, 2018

@author: x217777
'''

import os
import pandas as pd


def read_excel_data(file_path = "D:\\fmdbAutomationTestCases\\fromDipika"):
    os.chdir(file_path)
    file_name = 'testdata.xlsx'
    input_testdata = pd.read_excel(file_name,sheet_name=0,header=0,dtype=object)
    input_testdata.set_index('test_id',inplace = True)
    config_file_dict = input_testdata.T.to_dict()
    
    return config_file_dict
    #print(config_file_dict['25']['cat_no'])
    
    
read_excel_data()