'''
Created on Jul 23, 2017

@author: x217777
'''

import pandas as pd
import re
import collections
from database import DB





pd.set_option('display.height', 5000)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)
pd.set_option('display.max.colwidth',26)


class GenerateFile(object):
    ''' This class will execute read test data file, db query execution and then produce excel output file '''
    
    
    final_dataframe  = collections.OrderedDict()
    final_dataframe_len = collections.OrderedDict()
   
    
    def __init__(self,config_file):

        ''' Class constructor - special method '''
            
        self.config_file = config_file
        self.cat_num  = self.config_file['cat_no']
        

    def read_query_file(self):
        ''' Read input test data file based on Current or expire self.tag '''
        
        self.tag = 'Tag'
        self.current = 'current'
        self.expired = 'expired'
        current_fare_db_query_file = 'CurrentQueryFile'
        expire_fare_db_query_file = 'ExpireQueryFile'
        
        
        try:
        
            if self.config_file[self.tag] == self.current:
        
                self.xls_file = pd.ExcelFile(self.config_file[current_fare_db_query_file])
                self.first_sheet = self.xls_file.parse('Sheet1')
                #print('DB Query file {f} is parsed successfully\n'.format(f = self.config_file['CurrentQueryFile']))
        
            elif self.config_file[self.tag] == self.expired:
                
                self.xls_file = pd.ExcelFile(self.config_file[expire_fare_db_query_file])
                self.first_sheet = self.xls_file.parse('Sheet1')
                #print('DB Query file {f} is parsed successfully\n'.format(f = self.config_file['ExpireQueryFile']))
        
            else:
        
                raise "Incorrect Tag in config file"
                ##print('Incorrect self.tag specified in config file {self.tag}'.format(self.tag = self.config_file['Tag']))
              
        except IOError as fileError:
                      
            raise fileError
        
    def is_avfm_fare(self):
        
        try:
                # Parse the first_sheet where DB queries are listed
                
                
            if self.config_file[self.tag] == self.current and self.cat_num in ('002','005','011','014','015'):
                
                self.db_query_list = self.first_sheet.values[:]
                #print('All query',self.db_query_list)
                
                if len(self.db_query_list) == 8: # Current query file
       
                    # step - Static list of steps involved in exeuction
                    self.step = ['FARE','REC1','FTNT','FARERULE','ALTRULE','GENRULE','View','Avfm']

                    
                else:
                    raise 'This is AVFM fare, expecting 8 db query. Looks like few are missing'
                        
                     
                        
        except Exception as e:
            
            raise e
        
        
    def is_current_fare(self):
        
        if self.config_file[self.tag] == self.current and self.cat_num not in ('002','005','011','014','015'):
                    
            self.db_query_list = self.first_sheet.values[0:7]
           
            if len(self.db_query_list) == 7: # Current query file
            
                self.step = ['FARE','REC1','FTNT','FARERULE','ALTRULE','GENRULE','View']


                
            else:
                raise 'This is current fare but AVFM is not  applicable for this CAT, expecting 7 db query. Looks like few are missing'            


    def is_expire_fare(self):
        
        if self.config_file[self.tag] == self.expired:
                    
            self.db_query_list = self.first_sheet.values[:]
            #print('All query',self.db_query_list)
            
            if len(self.db_query_list) == 7: # Expired query file
            #print("All DB queries are parsed from file")
                self.step = ['FARE','REC1','FTNT','FARERULE','ALTRULE','GENRULE','View']

            else:
                raise 'This is expired fare, expecting 7 db query. Looks like few are missing'
            
        else:
            raise 'DB query file parsing failed, either incorrect tag or cat number '
    
    
    
    def fare_type(self):
        
        if self.config_file[self.tag] == self.expired:
            self.is_expire_fare()
            
        elif self.config_file[self.tag] == self.current and self.cat_num in ('002','005','011','014','015'):
            
            self.is_avfm_fare()
                   
        elif self.config_file[self.tag] == self.current and self.cat_num not in ('002','005','011','014','015'):
            self.is_current_fare()
            
        else:
            raise 'Wrong fare type'
                
    
    
    
    def set_excel_writer(self):
        
        try:   
            
            self.writer = pd.ExcelWriter(self.config_file['OutPutFilename'], engine='xlsxwriter')
        
        except Exception as e:
        
            raise e
        
        
    def excel_formatter(self,count):
        # Get the xlsxwriter workbook and worksheet objects.
        workbook  = self.writer.book
        
        
        worksheet1 = self.writer.sheets['Sheet{c}'.format(c=count)]

        # Add some cell formats.
        format1 = workbook.add_format({'align':'center'})
        # Set the column width and format.
        worksheet1.set_column('B:CD', 27, format1)                    
        
                                    
    def prepare_dbquerys(self):
        
        self.dbquery_dict = collections.OrderedDict()
            
        for count,query in enumerate(self.db_query_list):
              
            # Each item is ndArray(list), so accessed by index zero(0)
            dbquery = query[0]
            
            
            if 'xml' not in dbquery:
            # Substitute the CAT in DB query
                dbquery = re.sub('_cat.', '_cat{c}'.format(c=self.cat_num.strip('0')), dbquery)
                
            else:
                dbquery = re.sub('cat._xml', '(cat{c}_xml)'.format(c=self.cat_num.strip('0')), dbquery)
   
            for key,value in self.config_file.items():
               
                # if key in dbquery then replace with values from config file
                if key in dbquery:
                    dbquery = (dbquery.replace((':' + key), "'" + str(value) + "'" ))
                    
                    
            #self.dbquery_dataframe1 = dbquery_dataframe.append({self.step[count]: dbquery}, ignore_index=True)
            self.dbquery_dict[count] = dbquery

    def execute_dbquerys(self): 
               
        try:   
            
            db = DB()
            
            connection = db.get_connection()
       
            # Append new query with values into emtyDf then assigned to new datafreame i.e df
            for count , query in self.dbquery_dict.items():
               
                series = pd.Series(query)
                
                frame = series.to_frame(name=None)

                row=3

                frame.to_excel(self.writer, sheet_name='Sheet{c}'.format(c=count),index=False,startrow= row)

                row = row + len(frame.index) + 2
            
                # Execute newly form query against DB & write output to excel file
                dbquery_output = pd.read_sql_query(query, connection)
                
                dbquery_output.to_excel(self.writer, sheet_name='Sheet{c}'.format(c=count),startrow=row,startcol=0)
                
                
                # if Fare query then rename the listed column as per view requirement.
                if count == 0 and len(dbquery_output) != 0:
                    dbquery_output.rename(columns={'TAR_NO':'FARE_TAR_NO','SEQ_NO':'FARE_SEQ_NO','RM_MCN':'FARE_MCN','LOAD_TRANS':'FARE_LOAD_TRANS','EXPIRE_TRANS':'FARE_EXPIRE_TRANS'},inplace=True )
  
                    self.fare_loadcycle = dbquery_output.LOAD_CYCLE[0]
                    
                    self.fare_expirecycle = dbquery_output.EXPIRE_CYCLE[0]

                # Eliminate the duplicate columns if any
                dataframe = dbquery_output.loc[:,~dbquery_output.columns.duplicated()]

                self.final_dataframe['{s}'.format(s=self.step[count])] = dataframe
                
                self.final_dataframe_len['{s}'.format(s=self.step[count])] = len(dataframe)
                
                self.excel_formatter(count)
                
        except Exception as e:
            
            raise e
        
            
        finally:
            
            self.writer.save()
            
            db.close_connection()
            
            

     
        

    
