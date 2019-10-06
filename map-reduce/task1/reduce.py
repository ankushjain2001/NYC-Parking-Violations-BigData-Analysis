#!/usr/bin/env python

# Task 1 Reducer
import sys
data=[]
p_id=set()
o_id=set()

for line in sys.stdin:
    #print(line)
    line = line.strip()
    line = line.split('\t')
    line = [line[0]]+line[1].split('|')
    if line[5] == 'P':
        data.append(line)
        p_id.add(line[0])
    elif line[5] == 'O':
        o_id.add(line[0])

unpaid = p_id.intersection(o_id)

for i in data:
    if i[0] not in unpaid:
        print(i[0]+'\t'+i[1]+', '+i[2]+', '+i[3]+', '+i[4])

