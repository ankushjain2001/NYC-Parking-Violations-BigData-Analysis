#!/usr/bin/env python

# Task 2 Reducer
import sys
data=dict()

for line in sys.stdin:
    line = line.strip()
    code, count = line.split('\t')
    if code not in data:
        data[code] = int(count)
    else:
        data[code] += int(count)

for key, val in data.items():
    print(key+'\t'+str(val))

