'''
Created on Mar 16, 2018

@author: x217777
'''
'''
Created on Aug  2017

@author: x217777
'''

import unittest

from proboscis import test
from proboscis.asserts import assert_equal
from proboscis import TestProgram
from excel_data import read_excel_data
from test_executer import Execute






config_file_dict = read_excel_data()

execute = Execute()



#@test
def CAT3_AGR_GR_Expected_AGR():

    result = execute.testcase(config_file_dict['25'],'ALTRULE')
    assert_equal(result,'PASS')

@test
class CAT2(unittest.TestCase):
    '''DOM_CAT2_TestCases'''
    
        
    def test_CAT2_AGR_Expected_AGR(self):
         
        result = execute.testcase(config_file_dict['2'],'ALTRULE')
        assert_equal(result,'PASS') 


        
    """    
    def test_CAT2_AGR_But_AGR_NOAPP_Expected_NOKeyFound(self):
         
        result = execute.testcase(config_file_dict['3'],'No Key Found')
        assert_equal(result,'PASS')  
        
    def test_CAT2_FR_Expected_FR(self):
         
        result = execute.testcase(config_file_dict['4'],'FARERULE')
        assert_equal(result,'PASS') 
        
    """ 
        
        
#     def test_CAT2_FR_AGR_And_GR_Expected_FR_And_AGR(self):
#          
#         result = execute.testcase('DOM CAT2- FR and GR and AGR.yml','FARERULE')
#         assert_equal(result,'PASS') 



    
      
@test
class CAT3(unittest.TestCase):

    
#     Error
#     def test_CAT3_FR_AND_AGR_AND_GR_Expected_FR(self):
#          
#         result = execute.testcase(config_file_dict['23'],'FARERULE')
#         assert_equal(result,'PASS') 
        
        
    def DOM_CAT3_FTNT_AND_FR_AND_AGR_Expected_FTNT(self):
   
        result = execute.testcase(config_file_dict['24'],'FTNT')
        assert_equal(result,'PASS')
         
    
    def test_CAT3_AGR_GR_Expected_AGR(self):
         
        result = execute.testcase(config_file_dict['25'],'ALTRULE')
        assert_equal(result,'PASS')
        
        
    def test_CAT3_FR_AND_GR_Expected_FR(self):
          
        result = execute.testcase(config_file_dict['26'],'FARERULE')
        assert_equal(result,'PASS') 
        
    def DOM_CAT3_FR_AND_GR_But_GR_Negated_Expected_FR(self):
          
        result = execute.testcase(config_file_dict['27'],'FARERULE')
        assert_equal(result,'PASS')

  

@test
class CAT5(unittest.TestCase):
    '''DOM_CAT2_TestCases'''
    
    
    def test_CAT5_FR_AND_AGR_But_AGR_NOAPP_Expected_FR(self):
      
        result = execute.testcase(config_file_dict['5'],'FARERULE')
        assert_equal(result,'PASS')
    
    def test_CAT5_FR_But_FR_NOAPP_Expected_NOKeyFound(self):
         
        result = execute.testcase(config_file_dict['6'],'No Key Found')
        assert_equal(result,'PASS') 
 
    
    def test_CAT5_FR_Expected_FR(self):
         
        result = execute.testcase(config_file_dict['7'],'FARERULE')
        assert_equal(result,'PASS')  
    
  
    def test_CAT5_FR_AGR_GR_Expected_FR_AND_AGR(self):
         
        result = execute.testcase(config_file_dict['8'],'FARERULE','ALTRULE')
        assert_equal(result,'PASS')  
        
        
 
    def test_CAT5_AGR_GR_But_AGR_NOAPP_Expected_NOKeyFound(self):
          
        result = execute.testcase(config_file_dict['9'],'No Key Found')
        assert_equal(result,'PASS') 


@test
class CAT11(unittest.TestCase):
    '''DOM_CAT11_TestCases'''
    

#     Error    
#     def test_CAT11_FR_AGR_And_GR_Expected_FR_And_AGR(self):
#          
#         result = execute.testcase('DOM CAT11-FR and AGR and GR.yml','FARERULE','ALTRULE')
#         assert_equal(result,'PASS') 
#         
#     Error    
#     def test_CAT11_FR_Expected_FR(self):
#          
#         result = execute.testcase(config_file_dict['10'],'FARERULE')
#         assert_equal(result,'PASS')
#         
#     Error    
#     def test_CAT11_FTNT_FR_GR_Expected_Allsrc(self):
#       
#         result = execute.testcase(config_file_dict['11'],'FTNT','FARERULE','GENRULE')
#         assert_equal(result,'PASS') 
        
    def test_CAT11_AGR_GR__Expected_AGR(self):
         
        result = execute.testcase(config_file_dict['12'],'ALTRULE')
        assert_equal(result,'PASS')
        
    def test_CAT11_FR_GR_But_FR_NOAPP_Expected_NoKeyFound(self):
         
        result = execute.testcase(config_file_dict['13'],'No Key Found')
        assert_equal(result,'PASS')
        
        
    def test_CAT11_FTNT_FR_AGR_GR_Expected_Allsrc_Except_GR(self):
      
        result = execute.testcase(config_file_dict['14'],'FTNT','FARERULE','ALTRULE')
        assert_equal(result,'PASS')
        
        
    def test_CAT11_FR_GR_But_GR_NOAPP_Expected_FR(self):
         
        result = execute.testcase(config_file_dict['15'],'FARERULE')
        assert_equal(result,'PASS')
         
         
@test
class CAT14(unittest.TestCase):
    '''DOM_CAT14_TestCases'''
    
 
    def test_CAT14_FTNT_Expected_FTNT(self):
         
        result = execute.testcase(config_file_dict['16'],'FTNT')
        assert_equal(result,'PASS') 
        
        
    def test_CAT14_FR_Expected_FR(self):
         
        result = execute.testcase(config_file_dict['17'],'FARERULE')
        assert_equal(result,'PASS')
    
#     Error    
#     def test_CAT14_FTNT_AND_FR_Expected_FTNT(self):
#          
#         result = execute.testcase(config_file_dict['18'],'FTNT')
#         assert_equal(result,'PASS')
        
@test
class CAT15(unittest.TestCase):
    '''DOM_CAT2_TestCases'''    
  
    # Minor Fail about GR- RESTR_SRC_GEN_RULE_SRC
    def test_CAT15_FR_AND_GR_Expected_FR_AND_GR(self):
         
        result = execute.testcase(config_file_dict['19'],'FARERULE','GENRULE')
        assert_equal(result,'PASS') 
        
        
#     CAT15 rec3 has child table - need to take care
#     def test_CAT15_FTNT_AND_FR_AND_GR_But_GR_NOAPP_Expected_FTNT_AND_FR(self):
#           
#         result = execute.testcase(config_file_dict['20'],'FTNT','FARERULE')
#         assert_equal(result,'PASS') 


    def test_CAT15_FTNT_AND_FR_But_FR_NOAPP_Expected_FTNT(self):
         
        result = execute.testcase(config_file_dict['21'],'FTNT')
        assert_equal(result,'PASS') 
        
        
    def test_CAT15_AGR_But_AGR_NOAPP_Expected_NoKeyFound(self):
         
        result = execute.testcase(config_file_dict['22'],'No Key Found')
        assert_equal(result,'PASS') 
        
         
        
        
#@test
class OA(unittest.TestCase):
    '''DOM_CAT2_TestCases'''    
    
    #This fare is expired
#     def test_CAT14_FR_Expected_FR(self):
#          
#         result = execute.testcase('DOM CAT14-OA-FR restrictions should applied when only FR is potential source.yml','FARERULE')
#         assert_equal(result,'PASS') 
        
    # Expired fare - CAT15 rec3 has child table - need to take care
#     def test_CAT15_GR_Expected_GR(self):
#          
#         result = execute.testcase('DOM CAT15-OA-GR restrictions should applied when only GR is potential source.yml',1'GENRULE')
#         assert_equal(result,'PASS')
#         
        
    def test_CAT11_FR_But_FR_NOAPP_Expected_NoKeyFound(self):
         
        result = execute.testcase('DOM CAT11-OA-No restrictions should applied when FR is NoApp.yml','No Key Found')
        assert_equal(result,'PASS')
        
    def test_CAT5_FR_Expected_FR(self):
         
        result = execute.testcase('DOM CAT5-OA-FR restrictions should applied when FR is source.yml','FARERULE')
        assert_equal(result,'PASS')
        
    def test_CAT2_FR_Expected_FR(self):
         
        result = execute.testcase('DOM CAT2-OA-FR restrictions should applied when FR is source.yml','FARERULE')
        assert_equal(result,'PASS')
                          
TestProgram().run_and_exit()
    



