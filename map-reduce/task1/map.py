#!/usr/bin/env python

# Task 1 Mapper
import os
import sys
import string

csv_name = os.environ['mapreduce_map_input_file']

for line in sys.stdin:
    if 'open' in csv_name.lower():
        line = line.split(',')
        print('%s\t%s,%s,%s,%s,%s' % (line[0], '', '', '', '', 'O'))

    elif 'parking' in csv_name.lower():
        line = line.split(',')
        print('%s\t%s,%s,%s,%s,%s' % (line[0], line[14],line[6], line[2], line[1], 'P'))

