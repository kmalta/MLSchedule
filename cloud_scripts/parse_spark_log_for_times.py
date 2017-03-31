import sys
import math


cla = sys.argv
path = cla[1]
which_log = cla[2]

f = open(path + '/logs/log_trial_' + which_log, 'r')
f2 = open(path + '/epochs', 'a')

time_array = []
for line in f:
    if ': Job' in line:
        time_array.append(float(line.split()[-2]))

times_array = [time_array[0], time_array[1], time_array[2], sum(time_array[3:])]
f2.write(repr(time_array[3:]) + '\n')
