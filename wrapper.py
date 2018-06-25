'''
Created on Jun 12, 2018

@author: x217777
'''

import pandas as pd
from openpyxl import load_workbook
import time
from functools import wraps

class Utility:
    
    def to_excel(self,output_file,dataframe,stp,df_len):
        """ Output result to Excel function """
      
        try:
            
            book = load_workbook(output_file)
            writer = pd.ExcelWriter(output_file, engine='openpyxl') 
            writer.book = book
            writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
        
        except Exception as e:
            
            raise e
        
        else:
            dataframe.to_excel(writer, 'Sheet{st}'.format(st=stp),startrow= df_len + 10)
            writer.save()
            
    
    def stp_status(self,dataframe,stp,expect_restrn):
        """ Step Execution Status """
        
        self.status = []
        
        if 'FAIL' in dataframe['Result'].values:
            
            
            print('\nStep {st}:-View Validation against {t} table -> FAIL\n'.format(st=stp,t=expect_restrn))
 
            self.status.append('FAIL')
            
        else:
         
            self.status.append('PASS')
            print('\nStep {st}:-View Validation against {t} table -> PASS\n'.format(st=stp,t=expect_restrn))

      
    def tc_status(self):   
        """ Final i.e. TC Status """
        
        if 'FAIL' in self.status:

            return 'FAIL'
        
        else:
            
            return 'PASS'
        
        

    def time_this(self,func):
        '''
        Decorator that reports the execution time.
        '''
        @wraps(func)
        def wrapper(*args, **kwargs):
            
            start = time.time()
            result = func(*args, **kwargs)
            end = time.time()
            print(func.__name__, end-start)
            return result
        return wrapper

        
        
    
    