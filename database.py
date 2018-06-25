'''
Created on Jun 11, 2018

@author: x217777
'''

import cx_Oracle

class DB():
    
    def get_connection(self): 
        ''' Database Connection Method '''
    
        username = 'REVMGMT_FARE_ADMIN'
        password = 'REVMGMT_FARE_ADMIN99'
        database = 'RMFMCD_EX'
        
        try:
    
            self.connection = cx_Oracle.connect(username, password, database)
        
            #print('Connected to DB {db}\n'.format(db = self.config_file['Oracle']['host']))
        
        except cx_Oracle.DatabaseError as dberror:
    
            raise dberror
        
        else:
            
            return self.connection
        
        
    def close_connection(self):
        ''' Database Connection Close Method '''

        try:
            self.connection.close()
            
        except Exception as e:
            
            raise e 
        
