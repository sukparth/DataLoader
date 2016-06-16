# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 19:09:37 2016

@author: psukumar
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../")
print os.path.dirname(os.path.realpath(__file__))
import loader
import config
from database import  MySqlLoader
#import fnmatch
#import socket
import logging
import os
import sys
#import shutil

# Setup logging
logging.basicConfig(stream=sys.stdout, level=config.log_level)
log = logging.getLogger(__name__)

#from time import  strftime

class Test:
    def setup(self):
        pass
            
 
    def teardown(self):
        pass
         
 
    @classmethod
    def setup_class(cls):
        pass
 
    @classmethod
    def teardown_class(cls):
        pass
 
    def test_spec_dir_exist(self):
        print('Check existance of specification directory')
        try:
            if config.spec_dir: 
                print('Specification directory at {0}'.format(config.spec_dir))
            else:
                raise ValueError("spec_dir in config is empty")
        except:
                raise ValueError("Cannot find spec_dir path in config")
        try:
            if config.data_dir :
                pass
        except:
            raise ValueError("Cannot find data_dir path in config")
         
        try:
            if config.archive_dir :
                pass
        except:
            raise ValueError("Cannot find archive_dir path in config")

        
        print('Specification directory at {0}'.format(config.spec_dir))
        print('Data directory at {0}'.format(config.data_dir))
        print('Archive directory at {0}'.format(config.archive_dir))
        
    
    
    def test_single_loader(self):
         mysqlconn = MySqlLoader()
         mysqlconn.connect(config.mysqlconntest)
         cursor = mysqlconn.cnx.cursor()
         spec_file = os.path.join(config.spec_dir,"testformat2.csv")
         ifile = os.path.join(config.data_dir,"testformat2_2015-06-15.txt")
         table = "test_table"
         record_count = 100
         load_count_sql = "select count(1) from test_table"
         numloaded, numrejected = \
                                mysqlconn.load_table(spec_file,ifile,table,\
                                        create_table_if_not_exist = True, \
                                        truncate_data_before_load = True, \
                                        commit_number = None, \
                                        rollback_on_error = True, \
                                        buffer_len = 500, max_error = 0)
         print(str(numloaded),str(numrejected))
         cursor.execute(load_count_sql)
         row_count = cursor.fetchone()
         if row_count[0] != record_count:
             raise ValueError("number of rows loaded " + str(row_count[0]) +\
             " not equal to number of rows in file " + str(record_count))
         cursor.close()
         mysqlconn.disconnect()
    
    def test_archival(self):
         #for archive_file in os.listdir(config.archive_dir):
            #archive_file = os.path.join(config.archive_dir,archive_file)
            #shutil.move(archive_file,config.data_dir)
         #spec_file = os.path.join(config.spec_dir,"testformat1.csv")
         infile = os.path.join(config.data_dir,"testformat1_2015-06-15.txt")
         loader.move_file(infile,config.archive_dir)
            
         archive_file = os.path.join(config.archive_dir,"testformat1_2015-06-15.txt")
         if (not os.path.isfile(archive_file)) or os.path.isfile(infile):
                raise ValueError("Archival has failed")
         loader.move_file(archive_file,config.data_dir)
            
    
    
        
            
        

         
         
        
         
    

        

        
        
    
 