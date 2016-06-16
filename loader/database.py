# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 13:19:16 2016

@author: psukumar
"""


from __future__ import print_function
import config
import mysql.connector
from collections import namedtuple
import logging
import os
import sys
from codecs import open

from metadata import MetaDataInfo
from mysql.connector import errorcode

# Setup logging
logging.basicConfig(stream=sys.stdout, level=config.log_level)
log = logging.getLogger(__name__)


class MySqlLoader(object):
    """
    Class create connection and load data into MYSQL database
    Attributes
    -----------
    cnx --> represents the connection handler
    """
    _coltype = namedtuple('cinfo',
                          'name maxlen default_width minval maxval conv_func')
    _coltype_map = {
                    'TEXT': _coltype(name='TEXT', maxlen=64000000,
                                     default_width=None,
                                     minval=None, maxval=None, conv_func=str),
                    'BOOLEAN': _coltype(name='BOOL', maxlen=None,
                                        default_width=None,
                                        minval=None, maxval=None, 
                                        conv_func=bool),
                    'INTEGER': _coltype(name='INT', maxlen=4294967296,
                                        default_width=None,
                                        minval=-2147483648, maxval=2147483647,
                                        conv_func=int)
                    }
    
    @property
    def coltype(self):
        return self._coltype
    
    @property
    def coltype_map(self):
        return self._coltype_map
        
    def __init__(self, conn_dict=None):
        """ 
        Inititalizes the object
        Calls the connect method to create a database connection
        if connection dictionary with parameters is provided.
        
        Parameters
        ------------
        conn_dict -> connection dictionary with database parameters
        """
        
        if not conn_dict:
            self.cnx = None
        else:
            self.connect(conn_dict)

    def connect(self, conn_dict):
        """ 
        Inititalizes the object
        Creates a database connection if connection dictionary with parameters
        is provided.
        
        Parameters
        ------------
        conn_dict -> connection dictionary with database parameters
        """   
        
        assert conn_dict, "configuration details not provided"
        if not self.cnx:
            try:
                self.cnx = mysql.connector.connect(**conn_dict)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    log.exception("Access Denied, please check userid " + 
                                  "and/or password")
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    log.exception("Database does not exist")
                else:
                    log.exception(err)
                raise
            else:
                log.info("Connected to database")
        else:
            log.warning("Connection already established")

    def disconnect(self):
        """ Close database connection """
        
        if self.cnx:
            log.info("Disconnecting from database")
            self.cnx.close()
   
    def _column_sql(self, cname, cinfo):
        if cinfo.default_width:
            return cname + " " + cinfo.name + "(" + cinfo.default_width + ")"
        else:
            return cname + " " + cinfo.name
          
    def _create_table_sql(self, metadata_info, table, charset='utf8', 
                          engine='InnoDB'):
        assert metadata_info, "Table info object not povided"
        assert table, "Table name cannot be empty"
        create_sql = "create table " + " if not exists " + table + "("
        for cindex in range(0, len(metadata_info.fields)-1):
            assert metadata_info.fields[cindex].dtype in \
                    self._coltype_map, \
                    "data type {0} not valid". \
                    format(self._coltype_map[metadata_info.fields[cindex].dtype])                    
            create_sql += self._column_sql(metadata_info.fields[cindex].field, 
                          self._coltype_map[metadata_info.fields[cindex].dtype]) \
                          + " ,"
        assert metadata_info.fields[-1].dtype in  self._coltype_map, \
        "data type {0} not valid". \
        format(self._coltype_map[metadata_info.fields[-1].dtype])                  
        create_sql += self._column_sql(metadata_info.fields[-1].field, \
              self._coltype_map[metadata_info.fields[-1].dtype]) \
              + " )"
        create_sql += " engine=" + engine + " CHARACTER SET=" + charset + ";"
        return create_sql
    
    def _truncate_table_sql(self,table):
        log.info("Truncate table {0} as truncate option set ot True.".format(table))
        return "truncate table " + table + " ;"
        
    def _delete_table_all_rows_sql(self,table):
        return "delete from table " + table + " ;"
        
    def _create_table(self, metadata_info, table, cursor = None, \
                            charset = 'utf8', engine = 'InnoDB'):
        try:        
            assert self.cnx
        except AssertionError:
            log.exception("Database connection not opened")
            raise
        
        cursor_create = False
        if not cursor:
            cursor = self.cnx.cursor()
            cursor_create = True
            
        create_sql = self._create_table_sql(metadata_info,table,charset = charset, \
                                        engine = engine)
        log.info("Creating table {0} if it does not exist.".format(table))
        log.debug("Create table sql {0}".format(create_sql))
        try:
            cursor.execute(create_sql)        
        except mysql.connector.Error as err:
            log.exception(err.msg)
            raise
        else:
            log.debug("Created table {0}".format(table))
        
        if cursor_create:
            cursor.close()
    
    
    def _insert_table_sql(self,metadata_info,table,cursor = None):
        insert_sql_prefix = "INSERT INTO " + table + "("
        insert_sql_suffix = " VALUES ("
        for cindex in range(0,len(metadata_info.fields)-1):
            assert metadata_info.fields[cindex].dtype in \
                    self._coltype_map, \
                    "data type {0} not valid". \
                    format(self._coltype_map[metadata_info.fields[cindex].dtype])                    
            insert_sql_prefix += metadata_info.fields[cindex].field + " ,"
            insert_sql_suffix += "%s, "
        assert metadata_info.fields[-1].dtype in  self._coltype_map, \
        "data type {0} not valid". \
        format(self._coltype_map[metadata_info.fields[-1].dtype])                  
        insert_sql_prefix += metadata_info.fields[-1].field + " ) "
        insert_sql_suffix += "%s );"
        return insert_sql_prefix + insert_sql_suffix
        
        
    def exec_sql(self, sql, cursor = None, data = None, commit = False):
        """
        Executes the provided sql in the database.
        Parameters
        -----------
        sql --> sql statement to be executed
        cursor --> handler to run sql. A new one is created if not provided.
        data --> optional for bind variable sql's
        commit --> Commit database state once sql executed.
        """
       
        try:        
            assert self.cnx
        except AssertionError:
            log.exception("Database connection not opened")
            raise
        
        try:        
            assert sql
        except AssertionError:
            log.exception("Input sql not provided")
            raise
        
        
        cursor_create = False
        if not cursor:
            cursor = self.cnx.cursor()
            cursor_create = True
            #log.info("Setting cursor")
        
        try:
            if not data:
                cursor.execute(sql)
            else:
                cursor.execute(sql,data)
        except mysql.connector.Error as err:
            log.exception(err.msg)
            raise
        
        if commit:
            self.cnx.commit()
            
        if cursor_create:
            cursor.close()
    
        
    def load_table(self,input_spec_file,input_file,table,\
                    create_table_if_not_exist = False, \
                    truncate_data_before_load = False, \
                    commit_number = None, \
                    rollback_on_error = True, \
                    buffer_len = 1, max_error = 0, input_encoding="utf-8"):
        """
        Load database table with data and metadata file.
        Parameters
        -----------
        input_spec_file -> Input specification file
        input_file -> input data file
        table -> output table name
        create_table_if_not_exist -> default False, creates table if does not 
                                        exist.
        truncate_data_before_load -> Purge table data before load
                                     Default is False
        commit_number -> number of inserts after which data is to be committed.
                         Default is None which is commit after load.
        rollback_on_error -> Default is True.
                             True means transaction is rolled back if not 
                             committed and number of errors > max_error
        buffer_len -> Number of records to biffer before bulk load.
                      Default 1 which row by row insert.
                      
        Example:
        specification File:
        -------------------
        "column name",width,datatype
        name,10,TEXT
        valid,1,BOOLEAN
        count,3,INTEGER
        
        Input file:
        -----------
        Foonyor   1  1
        Barzane   0-12
        Quuxitude 1103
        """

        try:        
            assert self.cnx
        except AssertionError:
            log.exception("Database connection not opened")
            raise
            
        try:    
            assert input_file
        except AssertionError:    
            log.exception("Inputfile not provided")
            raise

        try:
            assert os.path.isfile(input_file)
        except:
            log.exception("Cannot access file {0}".format(input_file))
            raise
            
        try:    
            assert table
        except AssertionError:    
            log.exception("Table name not provided")
            raise
            
            
        try:
            cursor = self.cnx.cursor()
        except:
            log.exception("Cannot create DB cursor for load process")
            raise
        
        

        metadata_info = MetaDataInfo(metadata_file = input_spec_file)

            
        if create_table_if_not_exist:
            self._create_table(metadata_info,table, cursor = cursor, \
                                    charset = 'utf8',engine = 'InnoDB')
        
        if truncate_data_before_load:
            self.exec_sql(self._truncate_table_sql(table), cursor = cursor)
            
            
        log.info("Attempting to load {0} into table {1}" \
                    .format(input_file,table))
        try:
            min_buffer_len = config.min_buffer_len or 1
        except:
            min_buffer_len = 1
        
        try:
            max_buffer_len = config.max_buffer_len or 2000
        except:
            max_buffer_len = 2000
        
        if commit_number < 1:
            commit_number == None
            
        if commit_number:
            if commit_number < buffer_len:
                log.warning("Commit number {0} is lesser than buffer length {1}"\
                            .format(commit_number,buffer_len))
                log.warning("Setting buffer length to commit number {0}"\
                            .format(commit_number))
                buffer_len = commit_number
                            
        
        if buffer_len < min_buffer_len:
            log.warning("input buffer_len is lesser than  min_buffer_len" )
            log.warning("setting buffer_len to {0}".format(min_buffer_len))
            buffer_len = 1
            
        if buffer_len > max_buffer_len:
            log.warning("input buffer_len is greater than  max_buffer_len" )
            log.warning("setting buffer_len to {0}".format(max_buffer_len))
            buffer_len = max_buffer_len
            
            
        data_buffer = []
        bsize_counter = 0
        load_counter = 0
        error_counter = 0
        commit_counter = 0
        metadata_info = MetaDataInfo(metadata_file = input_spec_file)
        insert_stmt = self._insert_table_sql(metadata_info,table,cursor)
        with open(input_file, encoding = input_encoding) as ifile:
            for iline in ifile:
                ilist = []
                index_counter = 0
                for col_info in metadata_info.fields:
                    typecast_func = self.coltype_map[col_info.dtype].conv_func                    
                    ilist.append(typecast_func(iline[index_counter:(index_counter + col_info.width)]))
                    index_counter = index_counter + col_info.width
                data_buffer.append(ilist)
                bsize_counter += 1
                commit_counter += 1
                if bsize_counter == buffer_len:  
                    try:
                        cursor.executemany(insert_stmt, data_buffer)
                        load_counter += bsize_counter
                    except mysql.connector.Error as err:
                        error_counter += 1
                        log.exception(str(err))
                        if error_counter > max_error:
                            if rollback_on_error:
                                self.cnx.rollback()
                            raise ValueError("max error limit reached") 
                    if commit_number and commit_counter >= commit_number:
                        self.cnx.commit()
                        commit_counter = 0
                    data_buffer = []
                    bsize_counter = 0
                
            if data_buffer:
                try:
                    cursor.executemany(insert_stmt, data_buffer)
                    load_counter += len(data_buffer)
                except mysql.connector.Error as err:
                    error_counter += 1
                    log.exception(str(err))
                    if error_counter > max_error:
                        if rollback_on_error:
                            self.cnx.rollback()
                        raise ValueError("max error limit reached")
            self.cnx.commit()
            return (load_counter, error_counter)
