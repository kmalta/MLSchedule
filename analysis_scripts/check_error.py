import ast

path = '/Users/Kevin/MLSchedule/synth_ram_exp_1gb_h1_2_13_17_30_trials/experiment-2017-02-15-05-53-59_synth_data_2_0_4_machines_all_data_500_epochs_30_trials/epochs'

f = open(path, 'r')
data = f.readlines()

for i, line in enumerate(data):
    num = len(ast.literal_eval(line))
    if num != 500:
        print i
    else:
        print num