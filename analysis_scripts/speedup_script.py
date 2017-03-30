from scale_out_diff_overheads import *

data_fold = 'synth_cluster_scaling_exps_all_machine_counts_3_13_17_m1'

features = 3231961
log_features = Math.log10(features)

log_lower = int(Math.floor(log_features))
log_upper = int(Math.ceil(log_features))

log_weight_upper = log_upper - log_features
log_weight_lower = log_feature - log_lower


time_dict = {}
time_dict['url'] = {}
time_dict['comm'] = {}

def get_real_data():
    directs = get_directories('data/exps_1_28_17_all_data_30_trials')
    directs = filter(lambda x: if 'url' in x, directs)
    for direct in directories:
        f = open('data/url/' + direct + '/epochs', 'r')
        epochs = ast.literal_eval(f.readlines()[0])
        mach_num = int(direct.split('_')[3])
        time_dict['url'][mach_num] = epochs

def get_comm_data():
    directs = get_directories('data/url')
    for direct in directories:
        f = open('data/url/' + direct + '/epochs', 'r')
        epochs = ast.literal_eval(f.readlines()[0])
        mach_num = int(direct.split('_')[3])
        time_dict['url'][mach_num] = epochs






