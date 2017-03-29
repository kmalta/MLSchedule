import os
from dateutil import parser
import ast

def get_directories(root):
    try:
        for directory in os.walk(root):
            directories = directory[1]
            break
    except:
        directories = ''
    return directories


sample_dir = 'synth_exps_100k'

directories = get_directories(sample_dir)

dir_ = 'experiment-2017-01-06-09-51-29_synth_data_8_-5_2_machines_0_data_500_epochs_1_trials' #directories[-5]

f = open(sample_dir + '/' + dir_ + '/logs/log_trial_0', 'r')
log = f.readlines()
f.close()

f = open(sample_dir + '/' + dir_ + '/epochs', 'r')
epoch_sum = sum(ast.literal_eval(f.readlines()[0]))
f.close()

f = open(sample_dir + '/' + dir_ + '/all_iters', 'r')
all_iters = float(f.readlines()[0])
f.close()


flag2 = 0
time1 = 0
time2 = 0
line1 = ''
line1_idx = 0
line2 = ''
line1_idx = 0

diff = []

for i, line in enumerate(log):
    if ': Job ' in line and int(line.split(': Job ')[1].split()[0]) > 52:
        flag2 = 1
        if '[GC' in line:
            time1 = line.split()[2]
        else:
            time1 = line.split()[1]
        line1 = line
        line1_idx = i
    elif 'Starting job: ' in line and flag2 == 1:
        flag2 = 0
        if '[GC' in line:
            time2 = line.split()[2]
        else:
            time2 = line.split()[1]
        line2 = line
        line2_idx = i
        try:
            diff.append((parser.parse(time2) - parser.parse(time1)).total_seconds())
            #print (parser.parse(time2) - parser.parse(time1)).total_seconds()
        except:
            print 'EXCEPTION:', str(time1)
            print line1,
            print line1_idx
            print'EXCEPTION:', str(time2)
            print line2,
            print line2_idx
    if flag2 == 0:
        continue

print sum(diff)

print all_iters - epoch_sum
