import glob
from network_latency_experiment import *

def dump_train_file_meta_data(f):

    f2 = open('hostfile', 'r')
    ips = map(lambda x: x.strip(), f2.readlines())
    f2.close()

    for (i, ip) in enumerate(ips):
        if ip != '':
            py_ssh('', ip, 'wc -l ' + glob.REMOTE_PATH + '/data_loc/train_file.' + str(i) + ' > ~/dump_meta_data_file.' + str(i))
            py_scp_to_local('', ip, '~/dump_meta_data_file.' + str(i), 'dump_meta_data_file.' + str(i))
            out = py_out_proc('cat dump_meta_data_file.' + str(i))
            f.write(ip + '\n\n' + 'Training file lines: ' + out + '\n')
            f.write('\n\n\n')
            py_cmd_line('rm dump_meta_data*')


def create_dump_file():
    f = open('dump_file', 'w')
    f.write('##############################################################################\n')
    f.write('################################# DUMP FILE ##################################\n')
    f.write('##############################################################################\n')
    f.write('\n\n\n')
    dump_train_file_meta_data(f)


def main():
    glob.set_globals()
    create_dump_file()

if __name__ == "__main__":
    main()