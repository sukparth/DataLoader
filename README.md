## Synopsis

This can be used to load data files into MySQL database based on an input specification files.
The data will be picked up from the data folder based on the corresponding specifications in the specs folder.
Once loaded successfully the data will move to archive.
On failure it will move to reject.

## Pre-requisites
A Mysql database which is running and can be written into.


Official Mysql connector for python which can be downloaded from the below.
https://dev.mysql.com/downloads/connector/python/
Currently this is unavialable from  pypi and has to be manually intalled as per installation procedures documented in website.
https://dev.mysql.com/doc/connector-python/en/connector-python-installation.html

Install nose from pypi for tests


## Tests on
OS :- redhat 6.+/windows 7
python : 2.7


## Setup
git clone https://github.com/sukparth/DataLoader.git

cd DataLoader/loader

#############
Edit config.py with the correct database configuration for mysqlconn connection dictionary if needed.

The default parameters are as below
mysqlconn = {
  'user': 'loader',
  'password': 'loader',
  'host': 'localhost',
  'database': 'work',
  'raise_on_warnings': False,
  'charset': 'utf8',
  'use_unicode': True
}
################


## Run
python loader.py
