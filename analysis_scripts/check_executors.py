import os


def get_subdirs(root):
    directories = ''
    try:
        for directory in os.walk(root):
            directories = directory[1]
            break
    except:
        print "Could not get subdirs for root:", root
    return directories


root = 'synth_cluster_scaling_exps_all_machine_counts_3_13_17_h1'

subdirs = get_subdirs(root)

for dir_ in subdirs:
    for i in range(3):
        f = open(root + '/' + dir_ + '/logs/log_trial_'+ str(i), 'r')
        log_line = f.readlines()[17]
        log_num = int(log_line.split()[1])
        dir_num = int(dir_.split('_')[-8])
        if log_num != dir_num:
            print i, log_num, dir_