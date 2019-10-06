#!/usr/bin/env python

# Task 3 Reducer
import sys
import numpy
data=dict()

for line in sys.stdin:
    line = line.strip()
    line = line.split('\t')
    lic_type = line[0]
    amount, count = line[1].split('|')
    if lic_type not in data:
        data[lic_type] = [float(amount), int(count)]
    else:
        data[lic_type][0] += float(amount)
        data[lic_type][1] += int(count)

for key, val in data.items():
    print(key+'\t'+"{:0.2f}".format(val[0])+', '+"{:0.2f}".format(val[0]/val[1]))
