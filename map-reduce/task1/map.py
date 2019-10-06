#!/usr/bin/env python

# Task 1 Mapper
import os
import sys
import csv
from io import StringIO

csv_name = os.environ['mapreduce_map_input_file']

for line in sys.stdin:
    if 'open' in csv_name.lower():
        line = line.strip()
        reader = csv.reader(StringIO(line), delimiter=',')
        for row in reader:
            print('%s\t%s|%s|%s|%s|%s' % (row[0], '', '', '', '', 'O'))

    elif 'parking' in csv_name.lower():
        line = line.strip()
        reader = csv.reader(StringIO(line), delimiter=',')
        for row in reader:
            print('%s\t%s|%s|%s|%s|%s' % (row[0], row[14],row[6], row[2], row[1], 'P'))
