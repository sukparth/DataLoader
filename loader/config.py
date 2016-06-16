# -*- coding: utf-8 -*-
"""


@author: psukumar
"""
import os
import logging

log_level = logging.INFO

mysqlconn = {
  'user': 'loader',
  'password': 'loader',
  'host': 'localhost',
  'database': 'work',
  'raise_on_warnings': False,
  'charset': 'utf8',
  'use_unicode': True
}


mysqldb = {
   'default_charset': 'utf8',
   'default_engine': 'InnoDB'
}


min_buffer_len = 1
max_buffer_len = 2000
spec_file_column_list = ["column name", "width", "datatype"]
specs_pattern = "*.csv"
data_pattern = "*.txt"
spec_dir = None
data_dir = None
archive_dir = None
reject_dir = None
_spec_dirname = "specs"
_data_dirname = "data"
_archive_dirname = "archive"
_reject_dirname = "reject"
_filedir = os.path.dirname(__file__)
_goup = 3

# Search upto three parent level dirs from current dir in spec_dir has no value
if not spec_dir:
    while _goup > -1:
        spec_dir = os.path.join(_filedir, _spec_dirname)
        data_dir = os.path.join(_filedir, _data_dirname)
        archive_dir = os.path.join(_filedir, _archive_dirname)
        reject_dir = os.path.join(_filedir, _reject_dirname)
        if not os.path.isdir(spec_dir) or not os.path.isdir(data_dir):
            _spec_dirname = "../" + _spec_dirname
            _data_dirname = "../" + _data_dirname
            _archive_dirname = "../" + _archive_dirname
            _reject_dirname = "../" + _reject_dirname
            _goup -= 1
        else:
            break

    if _goup < 0:
        spec_dir = None
        data_dir = None
        archive_dir = None
        reject_dir = None


def main():
    print("spec_dir {}".format(spec_dir))
    print("data_dir {}".format(data_dir))
    print("archive_dir {}".format(archive_dir))
    print("reject_dir {}".format(reject_dir))

if __name__ == "__main__":
    main()
