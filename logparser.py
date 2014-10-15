#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author:             Nathan Farrar
# @Date:               2014-10-14 11:28:09
# @Last Modified by:   Nathan Farrar
# @Last Modified time: 2014-10-14 20:33:55

import csv
import logging
import json
import re
import sys
from datetime import datetime

import logging.config


class LogParser:
    """ Parse log records from a CSV file. """

    def __init__(self, name='Log Parser'):
        """ Take a path. Identify the dialect and field names from the
        first row. """

        self.name = name
        self.fields = None
        self.records = []

        # Cached values, from @property methods.
        self.num_records = None
        self.first_record_datetime = None
        self.last_record_datetime = None

    @property
    def record_count(self):
        if self.num_records == None:
            self.num_records = len(self.records)
        return self.num_records

    @property
    def log_start(self):
        if self.first_record_datetime == None:
            for row in self.records:
                dt = row['datetime']
                if self.first_record_datetime == None or dt < self.first_record_datetime:
                    self.first_record_datetime = dt
        return self.first_record_datetime

    @property
    def log_end(self):
        if self.last_record_datetime == None:
            for row in self.records:
                dt = row['datetime']
                if self.last_record_datetime == None or dt > self.last_record_datetime:
                    self.last_record_datetime = dt
        return self.last_record_datetime

    @property
    def log_duration(self):
        return self.last_record_datetime - self.first_record_datetime

    def load(self, filename="records.dat"):
        """ Load a serialized LogParser object from a saved pickle file. """
        pass

    def load_csv(self, filename="records.csv"):
        """ Parse log records from a CSV file. """

        try:
            csvfile = open(filename, 'rb')
            logger.debug('Opened ' + filename + " for reading.")

            # Identify the csv dialect when opening the file.
            # self.dialect = csv.Sniffer().sniff(csvfile.read(1024))
            # csvfile.seek(0)

            # CSV dialect detection the Sniffer failed, hardcoding the
            # dialect for now.
            dialect = csv.excel

            # Parse the fields out and normalize their names. Make everything
            # lowercase and replace spaces with underscores.
            self.fields = next(csvfile).rstrip().lower().replace(" ", "_").split(',')
            csvfile.seek(0)

            # Read the CSV as a dictionary, using the field mappings. For each row, add an
            # additional field 'datetime', containing a python datetime object.

            csv_reader = csv.DictReader(csvfile, fieldnames=self.fields, dialect=dialect)
            # Skip the header row.
            n = next(csv_reader)

            record_count = 0
            for row in csv_reader:
                row = next(csv_reader)
                row['datetime'] = LogParser.strtime_to_datetime(row['date_and_time'])
                self.records.append(row)
                record_count += 1
        except Exception, e:
            logger.error("Failed to open " + filename + " for reading.", exc_info=True)
            sys.exit(1)
        else:
            logger.info("Imported " + str(record_count) + " records from " + filename + ".")
            csvfile.close()


    def load_json(self, p="records.json"):
        pass

    def save(self, p="records.dat"):
        pass

    def save_csv(self, p="records.csv"):
        pass

    def save_json(self, p="records.csv"):
        with open(filename, 'w') as outfile:
            json.dump(self.records, outfile, default=LogParser.datetime_to_strtime)

        print "Saved " + str(self.record_count) + " records to " + filename + "."


    def summary(self):
        """ Display information about the csv file. """
        summary_data = {
            'Record Count': self.record_count,
            'First Record': self.log_start,
            'Last Record':  self.log_end,
            'Log Duration': self.log_duration
            }

        print ''.join([' ', self.name, ' ']).center(80, '-')

        for k, v in summary_data.items():
            print str(k).ljust(80 - len(str(v))) + str(v)

        print '-' * 80

        # print 'CSV Fields'
        # for field in self.fields:
        #     print '.. ' + field


    @staticmethod
    def line_count(filepath):
        """ Return the number of lines in a file. This is helpful for
        identifying if all records were imported, when lines and records have
        a 1-1 mapping. """

        try:
            f = open(filename)
            num_lines = sum(1 for line in f)
        except:
            print "The file " + filename + " could not be opened."
        finally:
            f.close()

        return num_lines

    @staticmethod
    def strtime_to_datetime(strtime):
        """ Return the datetime string as a native object.
        NOTE: The UTC offset is in a strange format. I'm preparsing it into
        the standard representation (superhack).
        """

        # Capture '+1:00' at end of time string.
        pattern = r"([\+|\-])(\d+):(\d\d)$"
        m = re.match(r".*" + pattern, strtime)

        # If we found a match, then rebuild the offset, using:
        # +0100' as the format. Replace the original occurrence.
        if m != None:
            offset = list(m.groups())
            offset[1] = offset[1].zfill(2)
            offset = ''.join(str(part) for part in offset)
            strtime = re.sub(r"([\+|\-])(\d+):(\d\d)$", "", strtime)

        # There's a bug in strptime ... docs say %z is supported, but it's
        # not. For now I'm discarding the offset.
        return datetime.strptime(strtime, '%d %b %Y %H:%M:%S %Z')

    @staticmethod
    def datetime_to_strtime(dto):
        """ Converts a datetime object back to a string, specifically for
        dumping the data to a json file."""

if __name__ == '__main__':
    logging.config.fileConfig('logging.ini')
    logger = logging.getLogger(__name__)

    csvfile = 'data/records.csv'

    p = LogParser()
    p.load_csv(csvfile)
    p.summary()

    # p.save_json()
    # p.save_pickle()

