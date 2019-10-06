#!/usr/bin/env python

# Task 2 Mapper
import os
import sys
import csv
from io import StringIO

csv_name = os.environ['mapreduce_map_input_file']

for line in sys.stdin:
    if 'parking' in csv_name.lower():
        line = line.strip()
        reader = csv.reader(StringIO(line), delimiter=',')
        for row in reader:
            print('%s\t%s' % (row[2], '1'))

