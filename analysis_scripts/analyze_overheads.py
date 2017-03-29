import os
import ast
import sys
import math
from sklearn import linear_model
import numpy as np

import warnings


warnings.simplefilter("ignore")


def read_log_metrics(strings):
    arr = filter(lambda x: 'first' not in x, strings)
    arr = map(lambda x: ast.literal_eval(x)[3], arr)
    return sum(arr)/len(arr)

def filter_datasets(machs, _directories):
    filtered = filter(lambda x: str(machs) + '_machine' in x, _directories)
    filtered_2 = filter(lambda x: 'data' not in x or '0_data' in x, filtered)
    return filtered_2[0]

def get_directories(root):
    try:
        for directory in os.walk(root):
            directories = directory[1]
            break
    except:
        directories == ''
    return directories

def predict(reg, p, machs, data, log_samples):
    temp = []
    temp.append(p[0])
    temp.append(p[1])
    temp.append(machs)
    temp.append(data)
    temp.append(log_samples)
    val_ = reg.predict(temp)[0]
    print 'Prediction:', str(val_), str(10**val_)

def analyze_real_data(dir_, num):
    f = open('exps_11_5_16/' + dir_ + '/all_iters', 'r')
    data = f.readlines()
    ave_iters = sum(map(lambda x: float(x), data))/len(data)
    f.close()
    f = open('exps_11_5_16/' + dir_ + '/log_metrics_file', 'r')
    log_metric = read_log_metrics(f.readlines()[1:])
    print num, ave_iters, log_metric, ave_iters - log_metric
    f.close()


#SCRIPT STARTS HERE!

clp = sys.argv
machs = int(clp[1])
dataset = clp[2]

b = []
p = []
one_dir = ''
two_dir = ''
four_dir = ''
sixteen_dir = ''
if dataset == 'url':
    b = [6, 7, -4, -5]
    p = [6.509466112, -4.446404784]
    log_samps = math.log10(2396130)
elif dataset == 'susy':
    b = [1, 2, 0, -1]
    p = [1.255272505, -0.005154530291]
    log_samps = math.log10(5000000)
elif dataset == 'higgs':
    b = [1, 2, 0, -1]
    p = [1.447158031, -0.0357133945]
    log_samps = math.log10(11000000)
elif dataset == 'kdda':
    b = [7, 8, -5, -6]
    p = [7.305713059, -5.745220403]
    log_samps = math.log10(8407752)
elif dataset == 'kddb':
    b = [7, 8, -6, -7]
    p = [7.475527295, -6.0071942]
    log_samps = math.log10(19264097)
else:
    sys.exit("Dataset Unrecognized.")

real_directories = get_directories('exps_11_5_16')

filtered_directories = filter(lambda x: dataset in x, real_directories)

one_dir = filter_datasets(1, filtered_directories)
two_dir = filter_datasets(2, filtered_directories)
four_dir = filter_datasets(4, filtered_directories)
try:
    sixteen_dir = filter_datasets(16, filtered_directories)
except:
    sixteen_dir = None


suffixes = ['10k', '100k', '1m']
log_suffs = [4, 5, 6]

X = []
y = []
for idx in range(0,3):
    suffix = suffixes[idx]
    directories = get_directories('synth_exps_' + suffix)
    for directory in directories:
        arr = directory.split('_')
        try:
            f = open('synth_exps_' + suffix +'/' + directory + '/log_metrics_file', 'r')
            metrics = ast.literal_eval(f.readlines()[1])
            f.close()
            f = open('synth_exps_' + suffix +'/' + directory + '/all_iters', 'r')
            total = float(f.readlines()[0])
            f.close()
        except:
            continue
        conf = map(lambda x: int(x), arr[3:6])
        if 'data' == arr[8]:
            conf.append(int(arr[7]))
        else:
            conf.append(0)
        conf.append(log_suffs[idx])
        if(conf[0] == b[0] or conf[0] == b[1]) and (conf[1] == b[2] or conf[1] == b[3]):
            diff = total - sum(metrics)
            X.append(conf)
            y.append(diff)

y = map(lambda x: math.log10(x), y)
X = np.asarray(X)
y = np.asarray(y)

reg = linear_model.Ridge(alpha = .5)
reg.fit(X, y)

predict(reg, p, 1, 0, log_samps)
predict(reg, p, 2, 0, log_samps)
predict(reg, p, 4, 0, log_samps)
predict(reg, p, 16, 0, log_samps)
print
analyze_real_data(one_dir, 1)
analyze_real_data(two_dir, 2)
analyze_real_data(four_dir, 4)
analyze_real_data(sixteen_dir, 16)