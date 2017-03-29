import sys
from dateutil import parser
import matplotlib.pyplot as plt

path = 'synth_ram_exp_8gb_h1_2_13_17_30_trials/experiment-2017-02-26-18-56-57_synth_data_7_-4_2_machines_all_data_500_epochs_1_trials/logs/log_trial_0'

f = open(path, 'r')
log = f.readlines()
f.close()

flag = 0
mach_1 = 61
mach_2 = 187

mach_1_task_starts = [0 for j in range(67)]
mach_2_task_starts = [0 for j in range(67)]
mach_1_task_stops = [0 for i in range(67)]
mach_2_task_stops = [0 for i in range(67)]
# res_starts = [0 for j in range(7)]
# res_stops = [0 for j in range(7)]

epoch = int(sys.argv[1])

for i, line in enumerate(log):

    if ': Job ' in line and int(line.split(': Job ')[1].split()[0]) >= epoch:
        flag = 1

    if ': Job ' in line and int(line.split(': Job ')[1].split()[0]) > epoch:
        flag = 0

    if flag != 0:
        if 'Starting task ' in line:
            arr = line.split()
            task = int(float(arr[-12]))


            stage = int(float(arr[-9]))

            machine_num = int(arr[-6].split('.')[-1].split(',')[0])
            time = arr[1]
            time = parser.parse(time)

            offset = 0
            if stage % 2 == 0:
                offset = 60
            if machine_num == mach_1:

                mach_1_task_starts[task + offset] = time
            else:
                mach_2_task_starts[task + offset] = time

        if 'Finished task ' in line:
            arr = line.split()
            task = int(float(arr[-12]))

            stage = int(float(arr[-9]))
            machine_num = int(arr[-2].split('.')[-1].split(',')[0])
            time = float(arr[-5])/1000

            offset = 0
            if stage % 2 == 0:
                offset = 60
            if machine_num == mach_1:
                mach_1_task_stops[task + offset] = time
            else:
                mach_2_task_stops[task + offset] = time

# start_1 = mach_1_task_starts[0]
# start_2 = mach_2_task_starts[0]

#mach_1_durs = [x - y - start_1 for x,y in zip(mach_1_task_stops, mach_1_task_stops)]
#mach_2_durs = [x - y - start_2 for x,y in zip(mach_2_task_stops, mach_2_task_stops)]


#print repr(mach_1_task_starts)
def plot_stages(ax, mach_start, mach_stop):
    start_tasks_with_indices = [x[0] for x in filter(lambda y: y[0][1] != 0 and y[1] != 0, zip(enumerate(mach_start), mach_stop))]

    start = start_tasks_with_indices[0][1]
    starts = [(x[1] - start).total_seconds() for x in start_tasks_with_indices]
    stops = [(x[1] - start).total_seconds() + mach_stop[x[0]] for x in start_tasks_with_indices]

    start_ids = [i[0] for i in sorted(start_tasks_with_indices, key=lambda x:x[1])]

    for i, j in enumerate(start_ids):
        ax.plot([starts[i], stops[i]], [j, j])


fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True)

#print repr([(x,y) for x,y in enumerate(mach_1_task_starts)])
#print mach_1_task_starts[29], mach_1_task_stops[29]

plot_stages(ax1, mach_1_task_starts, mach_1_task_stops)
plot_stages(ax2, mach_2_task_starts, mach_2_task_stops)

plt.title("Epoch: " + str(epoch))
plt.show()
