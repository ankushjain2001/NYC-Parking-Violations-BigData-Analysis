 #!/usr/bin/env python

# Task 2 Mapper
import os
import sys

csv_name = os.environ['mapreduce_map_input_file']

for line in sys.stdin:
    if 'parking' in csv_name.lower():
        line = line.split(',')
        print('%s\t%s' % (line[2], '1'))
