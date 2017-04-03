#import matplotlib.pyplot as plt
from scale_out_diff_overheads import *
from sklearn.linear_model import LinearRegression


#REFERENCE VALUES
datasets = ['higgs', 'susy', 'url', 'kdda', 'kddb']
datasets_bytes = [7.4, 2.3, 2.1, 2.5, 4.8]
samples = [11000000,5000000,2396130,8407752,19264097]
features = [28,18,3231961,20216830,29890095]



for k, dataset in enumerate(datasets):
    if k < 2:
        mach = 'm1'
    elif k < 4:
        mach = 'm1'
    else:
        continue

    machs = 7 if mach == 'h1' else 15


    feature = features[datasets.index(dataset)]

    data_fold = 'synth_cluster_scaling_exps_all_machine_counts_3_13_17_' + mach



    log_features = math.log10(feature)

    log_lower = int(math.floor(log_features))
    log_upper = int(math.ceil(log_features))

    log_weight_upper = log_upper - log_features
    log_weight_lower = log_features - log_lower


    time_dict = {}
    time_dict[dataset] = {}
    time_dict['comm'] = {}

    def get_real_data():
        directs = get_directories('data/computation-scaling-3-30-17/' + mach)
        directs = filter(lambda x: dataset in x, directs)
        for i in range(1, machs + 1):
            direct = filter(lambda x: str(i) + '_machines' in x, directs)[0]
            f = open('data/computation-scaling-3-30-17/' + mach + '/' + direct + '/epochs', 'r')
            epochs = ast.literal_eval(f.readlines()[0])
            f.close()
            f = open('data/computation-scaling-3-30-17/' + mach + '/' + direct + '/all_iters', 'r')
            time = float(f.readlines()[0])
            f.close()
            #mach_num = int(direct.split('_')[3])
            time_dict[dataset][str(i)] = {}
            time_dict[dataset][str(i)]['epochs'] = epochs
            time_dict[dataset][str(i)]['all_iters'] = time

    def get_comm_data():
        directs = get_directories('data/' + data_fold)
        directs = filter(lambda x: 'data_' + str(log_lower) in x or 'data_' + str(log_upper) in x, directs)

        for i in range(1, machs + 1):
            time_dict['comm'][str(i)] = {}

        for direct in directs:
            f = open('data/' + data_fold + '/' + direct + '/epochs', 'r')
            epochs = ast.literal_eval(f.readlines()[0])
            f.close()
            f = open('data/' + data_fold + '/' + direct + '/all_iters', 'r')
            time = float(f.readlines()[0])
            f.close()
            mach_num = int(direct.split('data')[1].split('_')[3])

            if 'data_' + str(log_upper) in direct:
                time_dict['comm'][str(mach_num)]['upper'] = {}
                time_dict['comm'][str(mach_num)]['upper']['epochs'] = epochs
                time_dict['comm'][str(mach_num)]['upper']['all_iters'] = time
            if 'data_' + str(log_lower) in direct:
                time_dict['comm'][str(mach_num)]['lower'] = {}
                time_dict['comm'][str(mach_num)]['lower']['epochs'] = epochs
                time_dict['comm'][str(mach_num)]['lower']['all_iters'] = time

    get_real_data()
    get_comm_data()

    #print log_weight_upper
    #print log_weight_lower

    comps = []
    comms = []
    reals = []

    for i in range(1, machs + 1):
        try:
            #comps.append(time_dict['url'][str(i)]['all_iters'] - (log_weight_upper*time_dict['comm'][str(i)]['upper']['all_iters'] + log_weight_lower*time_dict['comm'][str(i)]['lower']['all_iters']))
            comps.append(time_dict[dataset][str(i)]['all_iters'] - time_dict['comm'][str(i)]['lower']['all_iters'])
            reals.append(time_dict[dataset][str(i)]['all_iters'])
            comms.append(time_dict['comm'][str(i)]['lower']['all_iters'])

        except:
            print i
            sys.exit()

    # print comps[9]
    # print time_dict['comm']['9']['upper']['all_iters']
    # print time_dict['comm']['9']['lower']['all_iters']
    # print time_dict['url']['9']['all_iters']


    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, sharex=True)
    ax1.set_title('Epoch Times')

    ax2.set_title('Overheads + Communication Time')
    ax3.set_title('Computation Times')


    # estimate = np.median(real_comp[18][mach_plot_idx][10:50])
    # actual = sum(real_comp[18][mach_plot_idx])
    # estimated = sum(real_comp[18][mach_plot_idx][:50]) + estimate*450
    # perc_error = (math.fabs(actual - estimated)/math.fabs(actual)) * 100

    xaxis = [i + 2 for i in range(machs - 1)]

    lr_comm = LinearRegression()
    lr_comm.fit(np.transpose(np.matrix(xaxis)), np.transpose(np.matrix(comms[1:])))
    comm_predictions = lr_comm.predict(np.transpose(np.matrix(xaxis)))

    ax2.plot(xaxis, comms[1:], 'black')
    ax2.plot(xaxis, comm_predictions, 'red')



    lr_real = LinearRegression()
    lr_real.fit(np.transpose(np.matrix(xaxis)), np.transpose(np.matrix(reals[1:])))
    real_predictions = lr_real.predict(np.transpose(np.matrix(xaxis)))


    lr_samples = LinearRegression()
    lr_samples.fit(np.transpose(np.matrix([2,4,8])), np.transpose(np.matrix([reals[1], reals[3], reals[7]])))
    sample_predictions = lr_samples.predict(np.transpose(np.matrix(xaxis)))

    ax1.plot(xaxis, reals[1:], 'blue')
    ax1.plot(xaxis, real_predictions, 'red')
    ax1.plot(xaxis, sample_predictions, 'green')

    smooth_comp = [x - y for x,y in zip(real_predictions, comm_predictions)]
    predict_comp = [x - y for x,y in zip(sample_predictions, comm_predictions)]
    ax3.plot(xaxis, smooth_comp, 'purple')
    ax3.plot(xaxis, predict_comp, 'cyan')

    plt.suptitle(dataset + ' Speedup Curves')
    plt.show()


