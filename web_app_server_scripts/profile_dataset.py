from price_module.get_bid_API import *
from cloud_scripts.configure_hadoop_and_spark import *
from boto_launch_scripts import *
import math


inst_types = ['c4.xlarge', 'm4.xlarge', 'r4.2xlarge']
region = 'us-west-1'
cloud_config_path = 'cloud_configs/aristotle/aristotle-s3cfg'

#S3 URL
# s3://synth-datasets-2k/synth_data_1_0

def get_num_samples(path):
    os.system("wc -l " + path + " | awk '{print $1}' > temp_files/lines")
    f = open('temp_files/lines', 'r')
    total_lines = int(f.readlines()[0].strip())
    f.close()
    return total_lines

def get_stats(path):
    f = open(path, 'r')
    max_feats = 1

    count = 0
    running_feat_total = 0
    for line in f:
        count += 1
        try:
            arr = line.split()
            l = [int(elem.split(':')[0]) for elem in arr[1:]]
            if max_feats < l[-1]:
                max_feats = l[-1]
            running_feat_total += len(l)
        except:
            1

    return count, max_feats, float(running_feat_total)/count


def get_data_stats(cfg_file):
    f = open('data_configs/' + cfg_file + '.cfg', 'r')
    cfgs = f.readlines()
    f.close()

    s3url = cfgs[0].split('=')[1].strip()
    size_in_bytes = int(cfgs[1].split('=')[1].strip())
    samples = int(cfgs[2].split('=')[1].strip())
    features = int(cfgs[3].split('=')[1].strip())

    dataset_dict = {}
    dataset_dict['message'] = 'return profile data'
    dataset_dict['s3url'] = s3url
    dataset_dict['name'] = cfg_file
    dataset_dict['size_in_bytes'] = size_in_bytes
    dataset_dict['samples'] = samples
    dataset_dict['features'] = features
    dataset_dict['num_workers'] = 4;

    inst_type = inst_types[0]
    if math.log10(features) <= 4 and size_in_bytes < 4e9:
        inst_type = inst_types[0]
    elif math.log10(features) <= 7 and size_in_bytes < 11.5e9:
        inst_type = inst_types[1]
    else:
        inst_type = inst_types[2]


    ret_val = get_bid(inst_type, regions, 1)

    dataset_dict['machine_type'] = inst_type
    dataset_dict['bid'] = ret_val['bid']

    return dataset_dict


def get_data_stats_2(s3url):

    dataset = s3url.split('/')[-1]
    get_data = 's3cmd -d -v --force -c ' + cloud_config_path + ' get ' + s3url + '_0' +' temp_files/' + dataset


    while(1):

        os.system('rm temp_files/profile_s3_log')
        os.system(get_data + ' > temp_files/profile_s3_log')
        sleep(5)
        f = open('temp_files/s3_log', 'r')
        if '403' not in f.read():
            break
        print "Stuck in 403 Forbidden Loop"


    samples, features, average_sparsity = get_stats('temp_files/' + dataset)
    size_in_bytes = os.path.getsize('temp_files/' + dataset)
    ds_size = size(size_in_bytes, system=si)

    dataset_dict = {}
    dataset_dict['message'] = 'return profile data'
    dataset_dict['s3url'] = s3url
    dataset_dict['name'] = dataset
    dataset_dict['size'] = ds_size
    dataset_dict['samples'] = samples
    dataset_dict['features'] = features
    dataset_dict['sparsity'] = average_sparsity

    inst_type = inst_types[0]
    if math.log10(features) <= 4 and size_in_bytes < 4e9:
        inst_type = inst_types[0]
    elif math.log10(features) <= 7 and size_in_bytes < 11.5e9:
        inst_type = inst_types[1]
    else:
        inst_type = inst_types[2]


    ret_val = get_bid(inst_type, regions, 1)

    dataset_dict['machine_type'] = inst_type
    dataset_dict['bid'] = ret_val['bid']

    return dataset_dict

