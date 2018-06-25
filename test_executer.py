'''
Created on Feb 28, 2018

@author: x217777
'''
import view_validation_v2

class Execute(object):

    def testcase(self,config_file,cat_number,*src):
        ''' Method which takes filename, cat_number and restrictions source are applied '''

        d= view_validation_v2.DataValidation(config_file)
        d.read_query_file()
        d.fare_type()
        d.prepare_dbquerys()
        d.set_excel_writer()
        d.execute_dbquerys()
       
        d.tbl_restrns_date(*src)
        d.view_restrns_date()
        result = d.test_execution_flow(*src)

        return result
    
   
