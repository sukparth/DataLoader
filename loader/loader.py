# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 16:16:22 2016

@author: psukumar
"""
from database import  MySqlLoader
import config
import fnmatch
import socket
import logging
import os
import sys
import shutil
from time import  strftime


# Setup logging
logging.basicConfig(stream=sys.stdout, level=config.log_level)
log = logging.getLogger(__name__)


                
"""
   This script loads data into the mysql table.
   Log table entries are made before and after load.
   Data files are archived to archive folder once successful load.
   Data Files are moved to reject folder in case of error.
"""
                   
                
def move_file(ifile,move_dir):
    try:
        shutil.move(ifile, move_dir)
    except:
        log.exception("Cannot move file {0} to {1}".format(ifile,move_dir))
        raise

def pre_load_checks():
    try:
        if config.spec_dir: 
            pass
    except:
            log.exception("Cannot find spec_dir path in config")
            return False
    try:
        if config.data_dir :
            pass
    except:
        log.exception("Cannot find data_dir path in config")
        return False
     
    try:
        if config.archive_dir and config.reject_dir :
            pass
    except:
        log.exception("Cannot find archive_dir or reject_dir path in config")
        return False
        
    try:
        assert os.access(config.archive_dir, os.W_OK)
    except:
        log.exception("archive dir {0} not writable".format(config.archive_dir))
        return False
            
    try:
        assert os.access(config.reject_dir, os.W_OK)
    except:
        log.exception("Reject dir {0} not writable".format(config.reject_dir))
        return False
    
    return True

        
    
def data_loader():
    
    if not pre_load_checks:
        log.exception("Preload checks failed. Cannot begin load process.")
        raise Exception()
        
    try:
        specs_pattern = config.specs_pattern or "*.csv" 
    except:
        specs_pattern = "*.csv" 
        
    try:    
        speclist =  os.listdir(config.spec_dir)
    except:
        log.exception("Unable to get list of specification files from" + \
                        "specification directory {0}".format(config.spec_dir))
        raise Exception()
    
    if speclist:
        speclist = fnmatch.filter(speclist, specs_pattern)
    else:
        log.warning("Cannot find any specification files in " + \
                    " specification directory {0}".format(config.spec_dir))
        return
    
    try:    
        filelistall =  os.listdir(config.data_dir)
        if not filelistall:
            log.info("Cannot find any data files in " + \
                    " data directory {0}. Nothing to load." \
                    .format(config.spec_dir))
            return
    except:
        log.exception("Unable to get list of data files from" + \
                        "data directory {0}".format(config.data_dir))   
    

        
    mysqlconn = MySqlLoader()
    mysqlconn.connect(config.mysqlconn)
    create_log_table_ddl = "create table if not exists oplog" + \
                           "(loadproc VARCHAR(200)," + \
                           " hostname VARCHAR(100)," + \
                           " inputfile VARCHAR(400)," + \
                           " targettable VARCHAR(400)," + \
                           " startts TIMESTAMP NULL,endts TIMESTAMP NULL," + \
                           " numloaded BIGINT UNSIGNED," + \
                           " numerrored BIGINT UNSIGNED," + \
                           " status VARCHAR(40));"
                           
    log_start_sql = "insert into oplog(loadproc,hostname,inputfile" + \
                ",targettable,startts,status) values (%s,%s,%s,%s,%s,%s);"
    
    log_update_sql = "update oplog set endts = %s, status = %s, " + \
                     " numloaded = %s, numerrored = %s " + \
                     " where loadproc = %s " + \
                     " and  hostname = %s and inputfile = %s " + \
                     " and targettable = %s and startts = %s;" 
                     
    load_proc =  __file__
    host_name = socket.gethostname()           
    numloaded = None
    numrejected = None
    try:
        mysqlconn.exec_sql(sql = create_log_table_ddl)
    except:
        log.warning("Could not create log table if not exists")
    try:
        for spec_filename in speclist:
            log.info("The specfile {0} is ".format(spec_filename))
            data_pattern = os.path.splitext(spec_filename)[0] + config.data_pattern
            #print(data_pattern)
            #print(filelistall)
            filelist = fnmatch.filter(filelistall, data_pattern)
            #print(filelist)
            if filelist:
                log.info("List of files for specfile {0} is {1}" \
                        .format(spec_filename, filelist))
                spec_file = os.path.join(config.spec_dir,spec_filename)
                table =  os.path.splitext(spec_filename)[0] 
                for data_filename in filelist:
                    ifile = os.path.join(config.data_dir,data_filename)            
                    status = "running"
                    start_ts = strftime("%Y-%m-%d %H:%M:%S")
                    start_data = [load_proc,host_name,ifile,table, \
                                    start_ts, status]
                    try:
                        mysqlconn.exec_sql(sql = log_start_sql, data = start_data, \
                                            commit = True)
                    except:
                        log.warning("Cannot make an entry into log table")
                        
                    try:                        
                        numloaded, numrejected = \
                                mysqlconn.load_table(spec_file,ifile,table,\
                                        create_table_if_not_exist = True, \
                                        truncate_data_before_load = True, \
                                        commit_number = None, \
                                        rollback_on_error = True, \
                                        buffer_len = 500, max_error = 0)
                        status = "success"
                        log.info("Load of ifile {0} into table {1} completed" \
                                        .format(ifile,table))
                        log.info("Moving ifile {0} into archive_dir {1}" \
                                        .format(ifile,config.archive_dir))
                        move_file(ifile,config.archive_dir)
                    except:
                        status = "error" 
                        log.exception("Load of ifile {0} into table {1} faliled" \
                                        .format(ifile,table)) 
                        log.info("Moving ifile {0} into reject_dir {1}" \
                                        .format(ifile,config.reject_dir))
                        move_file(ifile,config.reject_dir)
                    finally:
                        end_ts = strftime("%Y-%m-%d %H:%M:%S")
                        update_data = [end_ts, status, numloaded, numrejected, \
                                       load_proc, host_name, ifile, table, \
                                       start_ts]
                        log.debug("Log data is ".format(update_data))
                        try:
                            mysqlconn.exec_sql(sql = log_update_sql, \
                            data = update_data, commit = True)
                        except:
                            log.warning("Cannot update log table with status")
    except:
        if mysqlconn.cnx:
            mysqlconn.disconnect()
        raise
        
                    
    if mysqlconn.cnx:
        mysqlconn.disconnect()
        
if __name__ == "__main__":
    data_loader()
        
        
        
        
        
        
    
    
        
        
        
        
        