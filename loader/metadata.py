# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 10:39:23 2016

@author: psukumar
"""

from __future__ import print_function
import csv
import config
import logging
import sys


logging.basicConfig(stream=sys.stdout, level=config.log_level)
log = logging.getLogger(__name__)


class FieldInfo(object):
    """
        Class for  field information
        Attributes :
            field --> field name
            width  --> width of field
            dtype  --> data type
    """
    __accepted_cols = set(['TEXT', 'BOOLEAN', 'INTEGER'])

    def __init__(self, field, width, dtype):
        """
            Initialize FieldInfo object
        """
        assert field, "Field name cannot be None or empty"
        assert width, "Width cannot be None or empty"
        assert dtype, "Data type cannot be None or empty"
        assert dtype in self.__accepted_cols, \
            "Data type {0} not in {1}".format(dtype, self.__accepted_cols)

        self.field = field
        try:
            self.width = int(width)
        except:
            raise ValueError("Width is not a  number")

        if self.width < 1:
            raise ValueError("Width is not a positive number")

        self.dtype = dtype


class MetaDataInfo(object):
    def __init__(self, fields=None, metadata_file=None):
        assert fields or metadata_file, "Fields cannot be empty"
        self.fields = []
        if metadata_file:
            self.fields = self.read_spec_file(metadata_file)
        else:
            self.fields = fields

    def __str__(self):
        print(self.fields)

    def read_spec_file(self, specfile):
        """ Read specification file in class object. """

        fieldlist = []

        try:
            spec_file_field_list = config.spec_file_field_list or \
                                ["column name", "width", "datatype"]
        except:
            spec_file_field_list = ["column name", "width", "datatype"]
        assert specfile, "Specfile is empty or None"

        with open(specfile) as csvf:
            specreader = csv.reader(csvf)  # Read csv
            header = next(specreader, None)
            log.debug("header is " + str(header))
            if header != spec_file_field_list:
                raise ValueError("Specification File")
            for row in specreader:
                fieldlist.append(FieldInfo(row[0], row[1], row[2]))
        return fieldlist
