'''
Created on Jul 23, 2017

@author: x217777
'''

from dbquery_execution_v1 import GenerateFile

from wrapper import Utility
import pandas as pd
import re
import datetime
import collections
import custom_logger as cl
import logging

class DataValidation(GenerateFile):
    ''' This class contains the various methods to filter and perform the actual data validation '''
    log = cl.customLogger(logging.DEBUG)

    def __init__(self,config_file):
        super().__init__(config_file)
        
        ''' Class constructor - special method '''
        self.config_file = config_file
        self.view_dict = collections.OrderedDict()
        self.view_restrn_date = collections.OrderedDict()
        self.tbl_restrn_date = collections.OrderedDict()
        self.lowest_seq = collections.OrderedDict()
        self.other_seq = collections.OrderedDict()
        self.cat_num = self.config_file['cat_no']
        self.executed = 0
        self.finalResult = 'FAIL'
      
        
        
    def single_seq_num(self, restrn):
        
        if (self.final_dataframe[restrn]['SEQ_NO'].nunique()) == 1:
  
            self.tbl_restrn_date[restrn]  = self.final_dataframe[restrn].loc[:,'LOAD_CYCLE':'EXPIRE_CYCLE'].copy()
            self.final_dataframe[restrn] = self.final_dataframe[restrn].sort_values(['SEG_IDX'], ascending= True)
            self.final_dataframe[restrn].reset_index(drop =True,inplace =True)
            
            
    def mul_seq_num(self,restrn):
         
        self.lowest_seq[restrn] = self.final_dataframe[restrn].loc[(self.final_dataframe[restrn]['SEQ_NO'] == self.final_dataframe[restrn]['SEQ_NO'].min()) ,:].copy()
        self.lowest_seq[restrn] = self.lowest_seq[restrn].sort_values(['SEG_IDX'], ascending= True)
        self.lowest_seq[restrn].reset_index(drop =True,inplace =True)
        self.other_seq[restrn] = self.final_dataframe[restrn].loc[(self.final_dataframe[restrn]['SEQ_NO'] != self.final_dataframe[restrn]['SEQ_NO'].min()) ,:].copy()
        
        
    def other_seq_num(self,restrn):
        
        if len(self.other_seq[restrn]['LOAD_CYCLE'] < self.fare_expirecycle) > 2 :
            
            '''
                if other seq has more than 2 records then perform below task
            '''
                    
            self.seq_tag = True
            self.other_seq[restrn] =  self.other_seq[restrn].loc[(self.other_seq[restrn]['LOAD_CYCLE'] < self.fare_expirecycle) ,:].copy()
            self.other_seq[restrn] .drop_duplicates(['LOAD_CYCLE','EXPIRE_CYCLE'],keep = 'first',inplace = True)
            self.other_seq[restrn] = self.other_seq[restrn].sort_values(['SEG_IDX'], ascending= True)
            self.other_seq[restrn].reset_index(drop =True,inplace =True)
            self.tbl_restrn_date[restrn]  = self.other_seq[restrn].loc[:,'LOAD_CYCLE':'EXPIRE_CYCLE'].copy()
            self.tbl_restrn_date[restrn].reset_index(drop =True,inplace =True)
      
        else :
        
            self.tbl_restrn_date[restrn] = self.lowest_seq[restrn].loc[:,'LOAD_CYCLE':'EXPIRE_CYCLE'].copy()
            self.tbl_restrn_date[restrn].reset_index(drop =True,inplace =True)
            
            
    def format_restrn_dates(self,restrn):
        
        for count,val in enumerate(self.tbl_restrn_date[restrn].index):
                
            if (count == 0) and (self.tbl_restrn_date[restrn].LOAD_CYCLE[count] != self.fare_loadcycle):
                
                '''
                    1st record - if restrn loadcycle date is less or great than fareload cycle then override with fareload cycle
                '''

                self.tbl_restrn_date[restrn].LOAD_CYCLE[count] = self.fare_loadcycle

            
            elif (count != 0) and (self.tbl_restrn_date[restrn].LOAD_CYCLE[count] < self.fare_loadcycle):
                
                self.tbl_restrn_date[restrn].LOAD_CYCLE[count] = self.fare_loadcycle
                
                
            if self.tbl_restrn_date[restrn].EXPIRE_CYCLE[count] == None and self.fare_expirecycle == None:
                '''
                     current fare, so set date to 2099-12-31
                '''
                self.tbl_restrn_date[restrn].EXPIRE_CYCLE[count] = '9912310000'
                
            elif self.tbl_restrn_date[restrn].EXPIRE_CYCLE[count] == None and self.fare_expirecycle != None:
                
                '''
                    expire  fare, so set date to expire_load cycle
                '''
                self.tbl_restrn_date[restrn].EXPIRE_CYCLE[count] = self.fare_expirecycle
       
        
            
        
    def restrn_date(self, restrn):
        ''' Method for restrn_date filter '''
        
        self.seq_tag = False
        
        if restrn in ('FTNT','FARERULE','ALTRULE','GENRULE') and len(self.final_dataframe[restrn]) != 0:

            if (self.final_dataframe[restrn]['SEQ_NO'].nunique()) > 1:
                
                self.mul_seq_num(restrn)
                
                self.other_seq_num(restrn)
        
            else: 
                self.single_seq_num(restrn)
                
                
            self.format_restrn_dates(restrn)
            
            
                
            self.tbl_restrn_date[restrn].rename(columns={'LOAD_CYCLE':'RESTRICTION_LOAD_TRANS','EXPIRE_CYCLE':'RESTRICTION_EXPIRE_TRANS'},inplace=True )
        
            self.tbl_restrn_date[restrn] = self.tbl_restrn_date[restrn].applymap(lambda x : datetime.datetime.strptime('20' + re.sub('[a-zA-Z.]', '', x) + '00','%Y%m%d%H%M%S'))
            
            '''
            lambda convert string to date for e.g 1st DOM.D16820.T2359(str) -> 201608202359(str) -> 2016-08-20 23:59:00 (date)
            string '20' and '00' added to original date string to match the date strptime format 
            
            '''
 
            if len(self.other_seq) != 0 and self.seq_tag  == True:
                restrn_len_before_concat = len(self.tbl_restrn_date[restrn])
                self.tbl_restrn_date[restrn] = pd.concat([self.tbl_restrn_date[restrn]] * self.lowest_seq[restrn].loc[1,'NUM_SEGS'])
                self.tbl_restrn_date[restrn] = self.tbl_restrn_date[restrn].sort_values(['RESTRICTION_LOAD_TRANS', 'RESTRICTION_EXPIRE_TRANS'], ascending=[True, False])
                self.tbl_restrn_date[restrn].reset_index(drop =True,inplace =True)
                #print(self.lowest_seq[restrn])
                self.lowest_seq[restrn] = pd.concat([self.lowest_seq[restrn]] * restrn_len_before_concat)
                self.lowest_seq[restrn].reset_index(drop =True,inplace =True)
                self.final_dataframe[restrn] = self.lowest_seq[restrn].copy()
                
            else:
                pass

        

    
    
    def view_restrns_date(self):
        
        """
        This function - filter/capture restrictions from view & add's them to  " viewdfdict" (dictionary) which can access later by key
        """
        
        for count,restrn in enumerate(self.final_dataframe.keys()):
            
            if count in (2,3,4,5):
                
                """
                2 == FTNT, 3 == FARERULE, 4 == ALTRULE, 5 == GENRULE
                """
                
                # View columns - pick only those are applicable to FTNT,FR, AGR and GR
                cols = list(self.final_dataframe['View'].loc[:,'RESTRICTION_LOAD_TRANS':'RESTRICTION_EXPIRE_TRANS']) + list(self.final_dataframe['View'].loc[:,'RESTRICTION_SRC':'UNAVAIL'])
                
                # Prepare New dataframe for each restriction in displayed in view
                self.view_dict[restrn] = self.final_dataframe['View'].loc[self.final_dataframe['View']['RESTRICTION_SRC'] == restrn,cols]
                
                # Reset index for newly created dataframe
                self.view_dict[restrn].reset_index(drop =True,inplace =True)
                
                # Drop duplicate from newly created dataframe if any
                self.view_dict[restrn].drop_duplicates(inplace = True )
                
                # capture load and expire trans/date for each restriction & keep in restriction date dataframe
                self.view_restrn_date[restrn] = self.final_dataframe['View'].loc[self.final_dataframe['View']['RESTRICTION_SRC'] == restrn,'RESTRICTION_LOAD_TRANS':'RESTRICTION_EXPIRE_TRANS']
                
                # Reset index for newly created dataframe
                self.view_restrn_date[restrn].reset_index(drop =True,inplace =True)
                
                #print(self.view_restrn_date[restrn])
                
            elif count == 6:
                
                """"
                Repeat above step for "No Key Found" if any
                """
                self.view_dict['No Key Found'] = self.final_dataframe['View'].loc[self.final_dataframe['View']['RESTRICTION_KEY'] == 'No Key Found','RESTRICTION_SRC':'UNAVAIL']
                self.view_dict['No Key Found'].reset_index(drop =True,inplace =True)
                self.view_restrn_date['No Key Found'] = self.final_dataframe['View'].loc[self.final_dataframe['View']['RESTRICTION_KEY'] == 'No Key Found','RESTRICTION_LOAD_TRANS':'RESTRICTION_EXPIRE_TRANS']
                #self.view_restrn_date['No Key Found'].set_index(keys = ['RESTRICTION_KEY'],drop =True,inplace =True)
               
        
  
    def tbl_restrns_date(self,*expect_restrns):
        
        """ Restriction i.e. FTNT, FR,AGR and GR filtering or Optimization function """

        for count,restrn in enumerate(self.final_dataframe.keys()):
            
            if 'No Key Found' not in expect_restrns:
        
                if count in (2,3,4,5) and restrn in expect_restrns:
                    
                    """
                    2 == FTNT, 3 == FARERULE, 4 == ALTRULE, 5 == GENRULE
                    """
                    
                    if  len(self.final_dataframe[restrn]) != 0 and (self.final_dataframe[restrn].NOAPPL.isnull().any()):
                        """
                        if table restrictions are not empty and NOAPLL is null for e.g. FR has records with NOAPP is null,
                        then call to restrn_date function to caputre restriction dates
                        """
                            
                        self.restrn_date(restrn)
                        
                elif count == 6:
                    pass
    
            elif 'No Key Found' in expect_restrns:
    
                    if count in (2,3,4,5) and restrn in expect_restrns and len(self.final_dataframe[restrn]) != 0 and (self.final_dataframe[restrn].NOAPPL.notnull().any()):
                        
                        self.restrn_date(restrn)
                
                    elif count == 6:
                        pass
    


    def view_vs_tbl_validation(self,srctbl,outtbl,oldsrctbllen,src,stp,m,n):
        """  View Vs TBL validation Function """
 
        
        
        print('*' * 120)
        self.log.info('*' * 120)
        print('\nStep {i} - View validation against {s} table\n'.format(i = stp, s=src))
        self.log.info('Step {i} - View validation against {s} table\n'.format(i = stp, s=src))
        df2 = pd.DataFrame(columns=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
        if len(srctbl) >= 0 :
            if self.executed <= len(src)+1:
                RESTR_SRC_REC2 = 'RESTR_SRC_REC2_'
                RESTR_SRC = 'RESTR_SRC_'
                
                for i in range(0,len(outtbl)):
                    if stp not in (0,1):
                        for col in (outtbl.columns[:]): 
                            
                            rep_RESTR_SRC_REC2 =col.replace(RESTR_SRC_REC2,'')
                            rep_RESTR_SRC =col.replace(RESTR_SRC,'')
                            
                            if col.startswith(RESTR_SRC_REC2) and rep_RESTR_SRC_REC2 in srctbl.columns:
                                
                                if  (outtbl.loc[i,col] == srctbl.loc[i,col.replace(RESTR_SRC_REC2,'')]):
                                    
                                    df = pd.Series([i,col,srctbl.loc[i,rep_RESTR_SRC_REC2],outtbl.loc[i,col] ,'PASS'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                    df2 = df2.append(df,ignore_index = True)
                                    
                                elif (outtbl.loc[i,col] != srctbl.loc[i,rep_RESTR_SRC_REC2]):
                                    
                                    df = pd.Series([i,col,srctbl.loc[i,rep_RESTR_SRC_REC2],outtbl.loc[i,col] ,'FAIL'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                    df2 = df2.append(df,ignore_index = True)
                                    
                                    
                                else:
                                    print('{c} view column not found in {s} information'.format(c = col,s = src))
                                    self.log.info('{c} view column not found in {s} information'.format(c = col,s = src))
                                
                            elif col.startswith(RESTR_SRC) and rep_RESTR_SRC in srctbl.columns:
                                if 'ALTRULE' in src:
                                    
                                    if rep_RESTR_SRC in ('GEN_RULE_SRC_TAR','GEN_RULE_RULE_NO'):
                                        #print(col)
                                        if outtbl.loc[i,col] == None:
                                            #print('None')
                                            df = pd.Series([i,col,srctbl.loc[i,rep_RESTR_SRC],outtbl.loc[i,col] ,'PASS'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                            df2 = df2.append(df,ignore_index = True)
                                        elif outtbl.loc[i,col] != None:
                                            if  (outtbl.loc[i,col] == srctbl.loc[i,col.replace(RESTR_SRC,'')]):
                                                df = pd.Series([i,col,srctbl.loc[i,rep_RESTR_SRC],outtbl.loc[i,col] ,'PASS'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                                df2 = df2.append(df,ignore_index = True)
                                                
                                            elif (outtbl.loc[i,col] != srctbl.loc[i,col.replace(RESTR_SRC,'')]):
        
                                                df = pd.Series([i,col,srctbl.loc[i,rep_RESTR_SRC],outtbl.loc[i,col] ,'FAIL'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                                df2 = df2.append(df,ignore_index = True)
                                                
                                    elif rep_RESTR_SRC not in ('GEN_RULE_SRC_TAR','GEN_RULE_RULE_NO'):
                                        
                                        if  (outtbl.loc[i,col] == srctbl.loc[i,col.replace(RESTR_SRC,'')]):
                                            #print('Not in col if ')
                                            df = pd.Series([i,col,srctbl.loc[i,rep_RESTR_SRC],outtbl.loc[i,col] ,'PASS'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                            df2 = df2.append(df,ignore_index = True)
                                            
                                        elif (outtbl.loc[i,col] != srctbl.loc[i,col.replace(RESTR_SRC,'')]):
                                            #print('Not in col elif')
                                            df = pd.Series([i,col,srctbl.loc[i,rep_RESTR_SRC],outtbl.loc[i,col] ,'FAIL'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                            df2 = df2.append(df,ignore_index = True)
                                            
    
                                else:   
                                    
                                    if  (outtbl.loc[i,col] == srctbl.loc[i,col.replace(RESTR_SRC,'')]):
        
                                        df = pd.Series([i,col,srctbl.loc[i,rep_RESTR_SRC],outtbl.loc[i,col] ,'PASS'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                        df2 = df2.append(df,ignore_index = True)
                                        
                                    elif (outtbl.loc[i,col] != srctbl.loc[i,col.replace(RESTR_SRC,'')]):
        
                                        df = pd.Series([i,col,srctbl.loc[i,rep_RESTR_SRC],outtbl.loc[i,col] ,'FAIL'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                        df2 = df2.append(df,ignore_index = True)
                                        
                                    else:
                                        print('{c} view column not found in {s} information'.format(c = col,s = src))
                                        self.log.info('{c} view column not found in {s} information'.format(c = col,s = src))
                                
                            
                            elif col in srctbl.columns:
                                 
                                if (outtbl.loc[i,col]) == (srctbl.loc[i,col]):
      
                                    df = pd.Series([i,col,srctbl.loc[i,col],outtbl.loc[i,col] ,'PASS'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                    df2 = df2.append(df,ignore_index = True)
                                    
                                elif (outtbl.loc[i,col]) != (srctbl.loc[i,col]):
     
                                    df = pd.Series([i,col,srctbl.loc[i,col],outtbl.loc[i,col] ,'FAIL'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                    df2 = df2.append(df,ignore_index = True)
                                    
                                else:
                                    print('{c} view column not found in {s} information'.format(c = col,s=src))
                                    self.log.info('{c} view column not found in {s} information'.format(c = col,s=src))
                                        
                                    
                            elif col not in srctbl.columns:
    
                                """ This code is for FARE_RULE_SEQ_ALT_GN_RULE and FARE_RULE_TAR_ALT_GN_RULE columns - for AGR only """
                                """ RESTR_SRC_GEN_RULE_SRC_TAR and other columns are from - FTNT """
                                #print(col)
                                
                                if col in ['FARE_RULE_SEQ_ALT_GN_RULE','FARE_RULE_TAR_ALT_GN_RULE','RESTR_SRC_GEN_RULE_SRC_TAR','RESTR_SRC_GEN_RULE_RULE_NO','RESTR_SRC_GEN_APPL','CAT15_CURRENCY']:
                                    
                                    if outtbl.loc[i,col] == None:
                                        df = pd.Series([i, col,None,outtbl.loc[i,col] ,'PASS'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                        df2 = df2.append(df,ignore_index = True)
    
                                    else:
                                        df = pd.Series([i, col,None,outtbl.loc[i,col] ,'PASS'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                        df2 = df2.append(df,ignore_index = True)
                                
                                elif col in [i,'RESTRICTION_SRC']:
                                    if outtbl.loc[i,'RESTRICTION_SRC'] == src:
                                        
                                        df = pd.Series([i,'RESTRICTION_SRC',src,outtbl.loc[i,'RESTRICTION_SRC'],'PASS'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                        df2 = df2.append(df,ignore_index = True)
                                    else: 
                                        df = pd.Series([i,'RESTRICTION_SRC',src,outtbl.loc[i,'RESTRICTION_SRC'],'FAIL'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                        df2 = df2.append(df,ignore_index = True)
                                        
                                        
                                elif col in [i,'RESTRICTION_KEY']:
                                    pass
                                
                                else:
                                    
                                    if outtbl.loc[i,col] == self.tbl_restrn_date[src].loc[i,col]:
                                        df = pd.Series([i,col,self.tbl_restrn_date[src].loc[i,col],outtbl.loc[i,col] ,'PASS'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                        df2 = df2.append(df,ignore_index = True)
                                    else:
                                        df = pd.Series([i,col,self.tbl_restrn_date[src].loc[i,col],outtbl.loc[i,col] ,'FAIL'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                        df2 = df2.append(df,ignore_index = True)
                                        
                                    
                    elif stp in (0,1):
                        for col in (outtbl.columns[0:46]):
                            if col in srctbl.columns:
                            
                                if (outtbl.loc[i,col]) == (srctbl.loc[0,col]):
                                    
                                    df = pd.Series([i,col,srctbl.loc[0,col],outtbl.loc[i,col] ,'PASS'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                    df2 = df2.append(df,ignore_index = True)
                                    
                                elif (outtbl.loc[i,col]) != (srctbl.loc[0,col]):
                                
                                    df = pd.Series([i,col,srctbl.loc[0,col],outtbl.loc[i,col] ,'FAIL'],index=['View_Record_Num','Column_Name','Expected({v}-value)'.format(v=src),'Actual(View-Value)','Result'])
                                    df2 = df2.append(df,ignore_index = True)
                                    
                            elif col in ['RESTRICTION_LOAD_TRANS','RESTRICTION_EXPIRE_TRANS']:#currently not recording these columns
                                
                                pass
                            
                            else:
                                print('{c} view column not found in {s} information'.format(c = col,s=src))
                                self.log.info('{c} view column not found in {s} information'.format(c = col,s=src))
                    
                print(df2)
                self.log.info('\n'+str(df2) + '\n')
                print('\n\n')
                #self.log.info('\n\n')
                
                self.utility = Utility()
                self.utility.to_excel(self.config_file['OutPutFilename'],df2, stp, oldsrctbllen)
                self.utility.stp_status(df2, stp, src)
                self.executed += 1
                
            else:
                self.utility.stp_status(df2, stp, src)
                
        else:
            print('Please cross check the query or test data as {} table is empty'.format(src))
            self.log.info('Please cross check the query or test data as {} table is empty'.format(src))
            
                

    def view_vs_avfm_validation(self,maindf, viewdf, dflen, src,count):
        
        print('*' * 100)
        self.log.info('*' * 120)
        print('\nStep 6 - View validation against AVFM  - > Starting\n')
        self.log.info('Step 6 - View validation against AVFM  - > Starting\n')
        
        
        self.maindf = maindf
        self.viewdf = viewdf
        self.final_dataframe_len = dflen
        self.src = src
        self.count = count
        
        self.viewdf = self.viewdf.replace(r'^\s+', str(None), regex=True)
        strAVFM = self.maindf['TO_CHAR((CAT._XML))'.replace('((CAT._XML))', '((CAT{c}_XML))'.format(c= self.cat_num))]
        strAVFM = strAVFM[0]
        
        

        if self.cat_num == 11:
            
            AvfmDict = {
                        
                'RESTRICTION_KEY'               : [],
                'RESTR_SRC_NUM_SEGS'            : [],
                'RESTRICTION_SRC'               : [],
                'RESTR_SRC_NOAPPL'              : [],
                'RESTR_SRC_RULE_FTNT_NO'        : [],
                'RESTR_SRC_TAR_NO'              : [],
                'RESTR_SRC_SEQ_NO': [],
                'RESTR_SRC_MCN': [],
                'RESTR_SRC_SEG_IDX': [],
                'RESTR_SRC_RI': [],
                'RESTR_SRC_TBL_NO': [],
                'RESTR_SRC_IO': [],
                'RESTR_SRC_DI': [],
                'RESTRICTED_DATES_APPL': [],
                'RESTRICTED_DATES_START': [],
                'RM_RESTRICTED_DATES_START_FMT': [],
                'RESTRICTED_DATES_STOP': [],
                'RM_RESTRICTED_DATES_STOP_FMT': [],
                'DATE_TBL_NO': [],
                'TEXT_TBL_NO': [],
                #'UNAVAIL'                   : [],

                }
            
        elif self.cat_num == 14:
            
            AvfmDict = { 
                             
            'RESTRICTION_KEY'           : [],
            'RESTRICTION_SRC'           : [],
            'RESTR_SRC_NOAPPL'          : [],
            'RESTR_SRC_NUM_SEGS'        : [],
            'RESTR_SRC_RULE_FTNT_NO'    : [],
            'RESTR_SRC_TAR_NO'          : [],
            'RESTR_SRC_SEQ_NO'          : [],
            'RESTR_SRC_MCN'             : [],
            'RESTR_SRC_SEG_IDX'         : [],
            'RESTR_SRC_RI'              : [],
            'RESTR_SRC_TBL_NO'          : [],
            'RESTR_SRC_IO'              : [],
            'RESTR_SRC_DI'              : [],
            'RM_TRAVEL_COMM_DT'         : [],
            'RM_TRAVEL_EXP_DT'          : [],
            'TRAVEL_APPL_IND'           : [],
            'RM_TRAVEL_COMM_COMPLETE_DT': [],
            'TRAVEL_TIME'               : [],
            'DATE_TBL_NO'               : [],
            'TEXT_TBL_NO'               : [],
            #'UNAVAIL'                   : [],
                            }
            
            
        elif self.cat_num == 15:
            
            AvfmDict = {
                        
            'RESTRICTION_KEY'               : [],
            'RESTR_SRC_NUM_SEGS'            : [],
            'RESTRICTION_SRC'           : [],
            'RESTR_SRC_NOAPPL'          : [],
            'RESTR_SRC_RULE_FTNT_NO'    : [],
            'RESTR_SRC_TAR_NO'          : [],
            'RESTR_SRC_SEQ_NO'          : [],
            'RESTR_SRC_MCN'             : [],
            'RESTR_SRC_SEG_IDX'         : [],
            'RESTR_SRC_RI'              : [],
            'RESTR_SRC_TBL_NO'          : [],
            'RESTR_SRC_IO'              : [],
            'RESTR_SRC_DI'              : [],
            'RM_EARLIEST_RES_DT'        : [],
            'RM_EARLIEST_TKTG_DT'       : [],
            'RM_LATEST_RES_DT'          : [],
            'RM_LATEST_TKTG_DT'         : [],
            'DATE_TBL_NO'               : [],
            'TEXT_TBL_NO'               : [],
            #'UNAVAIL'                   : [],
            
                    }

        
        
        
        
        
        elif self.cat_num == 5:
            
            AvfmDict = { 
                        
            'RESTRICTION_KEY'               : [],
            'RESTR_SRC_NUM_SEGS'            : [],
            'RESTRICTION_SRC'           : [],
            'RESTR_SRC_NOAPPL'          : [],
            'RESTR_SRC_RULE_FTNT_NO'    : [],
           'RESTR_SRC_TAR_NO'           : [],
            'RESTR_SRC_SEQ_NO'          : [],
            'RESTR_SRC_MCN'             : [],
            'RESTR_SRC_SEG_IDX'         : [],
            'RESTR_SRC_RI'              : [],
            'RESTR_SRC_TBL_NO'          : [],
            'RESTR_SRC_IO'              : [],
            'RESTR_SRC_DI'              : [],
            'ADV_RSVN_FIRST_TIME_OF_DAY': [],
            'ADV_RSVN_FIRST_PERIOD'     : [],
            'ADV_RSVN_FIRST_PERIOD_UNIT': [],
            'ADV_RSVN_LAST_TIME_OF_DAY' : [],
            'ADV_RSVN_LAST_PERIOD'      : [],
            'ADV_RSVN_LAST_PERIOD_UNIT' : [],
            'ADV_RSVN_PERM_IND'         : [],
            'ADV_TKTG_TIME_OF_DAY'      : [],
            'ADV_TKTG_PERIOD'           : [],
            'ADV_TKTG_PERIOD_UNIT'      : [],
            'ADV_TKTG_OPT_TIME'         : [],
            'ADV_TKTG_TIME_BEFORE_DEPARTURE' : [],
            'ADV_TKTG_TIME_BEFORE_DEPT_UNIT': [],
            'ADV_TKTG_BOTH_IND'         : [],
            'ADV_TKTG_EXCEPTION_TIME'   : [],
            'ADV_TKTG_EXC_TIME_UNIT'    : [],
            'DATE_TBL_NO'               : [],
            'TEXT_TBL_NO'               : [],
            'RM_WAIVER_RSVN_DT'         : [],
            'RM_WAIVER_TKTD_DT'         : [],
            #'UNAVAIL'                   : [],
            
            }

            
        elif self.cat_num == 2:
            
            AvfmDict = { 
                        
            'RESTRICTION_KEY'               : [],
            'RESTR_SRC_NUM_SEGS'            : [],
            'RESTRICTION_SRC'           : [],
            'RESTR_SRC_NOAPPL'          : [],
            'RESTR_SRC_RULE_FTNT_NO'    : [],
            'RESTR_SRC_TAR_NO'          : [],
            'RESTR_SRC_SEQ_NO'          : [],
            'RESTR_SRC_MCN'             : [],
            'RESTR_SRC_SEG_IDX'         : [],
            'RESTR_SRC_RI'              : [],
            'RESTR_SRC_TBL_NO'          : [],
            'RESTR_SRC_IO'              : [],
            'RESTR_SRC_DI'              : [],
            'TOD_START'                 : [],
            'TOD_STOP'                  : [],
            'TOD_APPL'                  : [],
            'DOW'                       : [],
            'DATE_TBL_NO'               : [],
            'TEXT_TBL_NO'               : [],
            'NEG'                       : [],
            #'UNAVAIL'                   : [],

            
            }
                
        
       
        
        if strAVFM:
            avfmList = re.findall(r'(\w+=\".+?\")', strAVFM)

        
        #print('NonEmptyDic',nonEmptyKeys)

        
            for item in avfmList:
     
                key, val = item.split('=',1)
                val = val.strip('"')
                val = val.strip()
                
                if val == '':
                    val = 'empty'
                
                if key in AvfmDict.keys() and not key.endswith('DT'):
                    AvfmDict[key].append(val)
            

                    
                elif key in AvfmDict.keys() and key.endswith('DT'):
                    
                    val = datetime.datetime.strptime(re.sub('[^0-9]' ,'', val) + '00','%Y%m%d%H%M')
                    AvfmDict[key].append(val)
 
                else:
                    
                    if key not in ['UNAVAIL']:
                        #print('This key not in require column', key)
                        raise 'Column {} is not in require column list'.format(key) 

            #fun to get max len key from dict,so later on can use to append empty [] 
            #same number of time to avoid error while Dict to Dataframe convertion        
            maxKeyLen = max(AvfmDict,key = lambda k: len(AvfmDict[k]))
            for key in AvfmDict.keys():
                if AvfmDict[key] == []:
                    for i in range(0, len(AvfmDict[maxKeyLen])):
                        AvfmDict[key].append(None)
                        
    
                    
            #print('Final Dict', AvfmDict)
            dfAVFM = pd.DataFrame(data=AvfmDict)
    
            dfAVFM = dfAVFM.sort_values(['RESTR_SRC_SEG_IDX'], ascending= True)
            dfAVFM.reset_index(drop =True,inplace =True)
    
            df2 = pd.DataFrame(columns=['View_Record_Num','Column_Name','Expected({v}-value)','Actual(View-Value)','Result'])
            
    
            for i in range(0,len(dfAVFM)):
                for column in (dfAVFM.columns[:]):
                    if column in self.viewdf.columns: #and (str( self.viewdf.loc[i,column]) != '' or str( self.viewdf.loc[i,column]) != None):
    
                        if  (str(dfAVFM.loc[i,column])).strip() == (str( self.viewdf.loc[i,column])).strip() :
                            
                            df1 = pd.Series([i,column,self.viewdf.loc[i,column],dfAVFM.loc[i,column] ,'PASS'],index=['View_Record_Num','Column_Name','Expected({v}-value)','Actual(View-Value)','Result'])
                            
                            df2 = df2.append(df1,ignore_index = True)
                            
                            #self.finalResult = 'PASS'
                            
                             
                        else:
    
                            
                            #print(column,self.viewdf.loc[i,column],dfAVFM.loc[i,column], type(self.viewdf.loc[i,column]),type(dfAVFM.loc[i,column]))
                            df1 = pd.Series([i,column,self.viewdf.loc[i,column],dfAVFM.loc[i,column] ,'FAIL'],index=['View_Record_Num','Column_Name','Expected({v}-value)','Actual(View-Value)','Result'])
                            df2 = df2.append(df1,ignore_index = True)
                           
                            
    
            
            df2.set_index('View_Record_Num',inplace=True)
           
            
            print(df2)
            self.log.info('\n'+str(df2)+'\n')
            
            self.utility.stp_status(df2, self.count, self.src)
            #self.result_to_excel(self.config_file, df2, self.count, self.final_dataframe_len)
            self.utility.to_excel(self.config_file['OutPutFilename'],df2, self.count, self.final_dataframe_len)
        
            
        else:
            if self.viewdf.loc[self.viewdf['RESTRICTION_KEY']] == 'No Key Found':
            
                print('AVFM restrictions are blank')
                #self.finalResult = 'PASS'
                
            else:
                print('View has other restrictions, expected No Key Found')
                
                
        
    def test_execution_flow(self,*src):
        """ Test Execution Function """

        self.log.info('#' * 120)
        self.log.info('Test Case - {t} - Execution -> Starting\n'.format(t = self.config_file['OutPutFilename']))
        
        if 'No Key Found' not in src:
          
            for count,restrn in enumerate(self.final_dataframe.keys()):

                if count == 0:
                    self.view_vs_tbl_validation(self.final_dataframe[restrn],self.final_dataframe['View'],self.final_dataframe_len[restrn],restrn,count,0,46)
                
                elif count == 1:
                    
                    print('*' * 100)
                    self.log.info('*' * 120)
                    print('\nStep {} - Currently {} is not captured for this test - > PASS\n'.format(count,restrn))
                    self.log.info('Step {} - Currently {} is not captured for this test - > PASS\n'.format(count,restrn))
                
                elif count in (2,3,4,5) and restrn in src:
    
                    self.view_vs_tbl_validation(self.final_dataframe[restrn],self.view_dict[restrn],self.final_dataframe_len[restrn],restrn,count,0,-1)
    
                elif count == 6:
                    print('*' * 100)
                    self.log.info('*' * 120)
                    print('\nStep 6 - Actual restrictions displayed in View\n')
                    self.log.info('\nStep 6 - Actual restrictions displayed in View\n')
                    print(self.final_dataframe['View'].T)
                    self.log.info(self.final_dataframe['View'].T)
                    print('*' * 100)
                    self.log.info('*' * 120)
                
                elif count == 7 and self.config_file['Tag'] == 'current' and self.cat_num in ('002','005','011','014','015'):
                    
                    #self.view_vs_avfm_validation(self.final_dataframe[restrn],self.final_dataframe['View'],self.final_dataframe_len[restrn],restrn,count)
                    pass
                       
                else:
                    print('*' * 100)            
                    self.log.info('*' * 120)
                    print('\nStep {} - {} is not applicable for this test - > PASS\n'.format(count,restrn))
                    self.log.info('Step {} - {} is not applicable for this test - > PASS\n'.format(count,restrn))
                  
            self.executed = 0
        
        else:
            
            for count,restrn in enumerate(self.final_dataframe.keys()):
                if count == 0:
                    self.view_vs_tbl_validation(self.final_dataframe['FARE'],self.final_dataframe['View'],self.final_dataframe_len['FARE'],'FARE',0,0,46)
                
                elif count in (2,3,4,5) and restrn in src:
                    print('*' * 100)
                    self.log.info('*' * 120)
                    print('\nStep 1 - > Very that Restriction Key should be "No Key Found" and other columns should be None - > Starting\n')
                    self.log.info('Step 1 - > Very that Restriction Key should be "No Key Found" and other columns should be None - > Starting\n')
                    if len(self.view_dict['No Key Found']) != 0:
                        
                        if self.view_dict['No Key Found'].dropna().empty:
                            
                            print(self.view_dict['No Key Found'])
                            #view_vs_tbl_validation(self.final_dataframe[restrn],self.view_dict['No Key Found'],self.final_dataframe_len[restrn],restrn,count,0,-1)
                            print('\nStep 1 - > Restriction Key is "No Key Found" and other columns are None - > PASS\n')
                            print('*' * 100)
                            self.log.info('*' * 120)
                            print('\nStep 2 - > Very that the Restriction Dates are correct - > Starting\n')
                            self.Restrictn_DT_Validation_View_Against_TBL(self.tbl_restrn_date[restrn],self.view_restrn_date['No Key Found'],restrn)
                            #print('\nStep 2 - > Very that the Restriction Dates are correct - > Completed\n')      
                        else:
                            print('Columns are not null - Test case is - > FAIL')
                            self.log.info('Columns are not null - Test case is - > FAIL')
                            
            
                    else:
                        print('\nStep 1 - > Very that Restriction Key should be "No Key Found" and other columns should be None - > FAIL due to incorrect test data\n')
                        self.log.info('\nStep 1 - > Very that Restriction Key should be "No Key Found" and other columns should be None - > FAIL due to incorrect test data\n')
                        
                    
                else:
        
                    pass
                
        
        result = self.utility.tc_status()
        
        print('\nTest Case - {t} - Execution -> FINAL_STATUS -> {r}\n'.format(t = self.config_file['OutPutFilename'],r=result))
        self.log.info('\nTest Case - {t} - Execution -> FINAL_STATUS -> {r}\n'.format(t = self.config_file['OutPutFilename'],r=result))
        self.log.info('#' * 120)
        
        return result
        
        
