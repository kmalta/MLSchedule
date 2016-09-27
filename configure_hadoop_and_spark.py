import glob
from deploy_cloud import *
from data_partition import *

import sys
from time import time
import os


import boto
from boto.ec2.regioninfo import RegionInfo
import boto.s3.connection

#GLOBALS
master_type='m3.2xlarge'
worker_type='m3.2xlarge'
master_ram=4096
worker_ram=4096
local_jar_path=['/Users/Kevin/MLSchedule/spark_dataframe_test/target/scala-2.11/ml-dataframe-test_2.11-1.0.jar', 
                '/Users/Kevin/MLSchedule/spark_dataframe_test_reload/target/scala-2.11/ml-dataframe-test-reload_2.11-1.0.jar',]
file_name='url_combined_0'
access_key="AKIAAT5XKN7CTKBYIJFF"
secret_key="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"
ec2_host="128.111.179.130"
s3_host="128.111.179.130"
bucket_name="url-combined-data"
data_set="url_combined"
region="race.cs.ucsb.edu"





def set_up_master(master_type):
    master_ip, master_id = launch_instance_with_metadata(master_type)
    return master_ip, master_id


def set_up_slaves(num_slaves, slave_types):
    launch_instances(num_slaves, slave_types)


def get_host(ip):
    py_ssh('', ip, 'source scripts/get_host.sh')
    py_scp_to_local('', ip, '/home/ubuntu/host', '~/MLSchedule/host')
    f = open('host', 'r')
    data = f.readlines()
    f.close()
    return data[0].strip(), data[1].strip()


def get_all_hosts(master_ip, ips):
    nodes_info = []
    master_host, master_priv_ip = get_host(master_ip)
    nodes_info.append([master_ip, master_host, master_priv_ip])
    for ip in ips:
        host, priv_ip = get_host(ip)
        nodes_info.append([ip, host, priv_ip])
    return nodes_info

def create_core_site_file(nodes_info, idx):
    py_ssh('', nodes_info[idx][0], 'cat hadoop_xml_files/core_site_beginning.xml host_master_tmp colon host_port_tmp hadoop_xml_files/core_site_ending.xml > hadoop_xml_files/core-site.xml')

def create_yarn_site_file(nodes_info, idx):
    port = 9000
    py_scp_to_remote('', nodes_info[idx][0], 'hadoop_xml_files/yarn_site_beginning.xml', 'hadoop_xml_files/yarn_site_beginning.xml')
    py_scp_to_remote('', nodes_info[idx][0], 'hadoop_xml_files/yarn_site_ending.xml', 'hadoop_xml_files/yarn_site_ending.xml')
    py_ssh('', nodes_info[idx][0], "echo -ne " + str(nodes_info[0][1]) + " > host_master_tmp; " + 
                                   "echo -ne : > colon; " + 
                                   "echo -ne " + str(port) + " > host_port_tmp")
    py_ssh('', nodes_info[idx][0], 'cat hadoop_xml_files/yarn_site_beginning.xml host_master_tmp hadoop_xml_files/yarn_site_ending.xml > hadoop_xml_files/yarn-site.xml')


def create_masters_file(nodes_info):
    f = open('masters', 'w')
    f.write(nodes_info[0][1] + '\n')
    f.close()

def create_slaves_file(nodes_info):
    f = open('slaves', 'w')
    count = 0
    for node in nodes_info[1:]:
        count += 1
        f.write(node[1] + '\n')
    f.close()

def create_etc_hosts_file(nodes_info, idx):
    f = open('hosts_file_beginning', 'w')
    f.write('127.0.0.1 localhost\n')
    #f.write('127.0.0.1 ' + nodes_info[idx][1] + '\n')
    f.write(nodes_info[0][2] + ' ' + nodes_info[0][1] + '\n')
    count = 0
    for node in nodes_info[1:]:
        count += 1
        f.write(node[2] + ' ' + node[1] + '\n')
    f.write('\n')
    f.close()
    os.system('cat hosts_file_beginning hosts_file_end > all_hosts_file')

def create_master_hdfs_site_file():
    1

def create_slave_hdfs_site_file():
    1

def setup_passwordless_ssh(nodes_info, master_port):
    py_ssh('', nodes_info[0][0], 'source /home/ubuntu/scripts/master_passwordless_ssh.sh '
           + glob.REMOTE_PATH + ' ' + glob.PEM_PATH + ' ' + nodes_info[0][0] + ' ' + 
           master_port + ' ' + nodes_info[0][1] + ' ' + nodes_info[0][2])


def hadoop_configs(nodes_info, up):
    if int(up) <= 2:
        create_masters_file(nodes_info)
        create_slaves_file(nodes_info)


        create_master_hdfs_site_file()
        create_slave_hdfs_site_file()


        i = 0
        for node in nodes_info:
            create_yarn_site_file(nodes_info, i)
            create_core_site_file(nodes_info, i)

            py_scp_to_remote('', node[0], 'hostfile', 'hostfile')
            create_etc_hosts_file(nodes_info, i)
            py_scp_to_remote('', node[0], 'all_hosts_file', 'all_hosts_file')
            i += 1


    if int(up) <= 4:
        for idx in range(len(nodes_info) - 1):
            configure_slave_node(nodes_info, idx + 1)

    if int(up) <= 5:
        configure_master_node(nodes_info)


def start_hadoop(master_ip):
    #remove on reimage
    py_scp_to_remote('', master_ip, 'scripts/start_hadoop.sh', 'scripts/start_hadoop.sh')
    py_ssh('', master_ip, 'source scripts/start_hadoop.sh')

def stop_hadoop(master_ip):
    #remove on reimages
    py_scp_to_remote('', master_ip, 'scripts/stop_hadoop.sh', 'scripts/stop_hadoop.sh')
    py_ssh('', master_ip, 'source scripts/stop_hadoop.sh')



def configure_master_node(nodes_info):
    master_ip = nodes_info[0][0]

    py_scp_to_remote('', master_ip, 'masters', 'masters')
    py_scp_to_remote('', master_ip, 'slaves', 'slaves')

    py_ssh('', master_ip, 'source scripts/all_env_vars.sh')

    setup_passwordless_ssh(nodes_info, str(7077))

    py_ssh('', master_ip, 'source scripts/namenode_env_vars.sh')

def configure_slave_node(nodes_info, idx):
    ip = nodes_info[idx][0]


    py_ssh('', ip, 'source scripts/all_env_vars.sh')
    py_ssh('', ip, 'source scripts/datanode_env_vars.sh ' + nodes_info[0][2] + ' ' + nodes_info[0][1])




def spark_config(nodes_info):
    f = open('spark_slaves', 'w')
    for node in nodes_info[1:]:
        f.write(node[0] + '\n')
    f.close()

    print "wrote conf/slaves file"

    py_scp_to_remote('', nodes_info[0][0], 'spark_slaves', 'spark-2.0.0/conf/slaves')
    for node in nodes_info:
        py_ssh('', node[0], 'echo -e export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop >> ~/spark-2.0.0/conf/spark-env.sh')


def start_spark(nodes_info):
    py_ssh('', nodes_info[0][0], 'spark-2.0.0/sbin/start-all.sh')

def stop_spark(nodes_info):
    py_ssh('', nodes_info[0][0], 'spark-2.0.0/sbin/stop-all.sh')

def scala_send_spark_job(nodes_info, jar_path):
    for node in nodes_info:
        py_scp_to_remote('', node[0], jar_path, 'jars/' + jar_path.split('/')[-1])

def scala_run_spark_job(nodes_info, master_port, file_name, hadoop_master_port, jar, algorithm, num_features):
    #NON-YARN
    run_str = ['/home/ubuntu/spark-2.0.0/bin/spark-submit --verbose --master ' + 
               'spark://' + nodes_info[0][1] + ':7077' + ' --deploy-mode client ' + 
               ' file:///home/ubuntu/jars/'+ jar + ' cluster ' + str(num_features) + 
               ' ' + nodes_info[0][1] + ' ' + str(hadoop_master_port) + ' ' + 
               file_name + ' ' + ' ' +  algorithm]

    #YARN
    # run_str = ['/home/ubuntu/spark-2.0.0/bin/spark-submit --verbose --master yarn' + 
    #            ' --deploy-mode cluster ' + ' file:///home/ubuntu/jars/'+ jar + ' cluster '
    #            + nodes_info[0][1] + ' ' + str(hadoop_master_port) + ' ' + file_name + ' ' +
    #            ' ' +  algorithm]

    print "@@@@@@@@@@@@@@@" + run_str[0]
    py_ssh('', nodes_info[0][0], run_str[0])




def run():

    clp = sys.argv

    up = clp[1]
    glob.set_globals()

    if int(up) == 0:
        inst_ids = check_instance_status('ids', 'all')
        inst_ips = check_instance_status('ips', 'all')
        if len(inst_ips) != 0:
            terminate_instances(inst_ids, inst_ips)

        master_ip, master_id = set_up_master(master_type)
        add_swapfile(master_ip, master_ram)
        f = open('host_master', 'w')
        f.write(master_ip + '\n')
        f.close()


    if int(up) <= 1:

        set_up_slaves(4, worker_type)
        inst_ips = check_instance_status('ips', 'all')
        inst_ips.remove(master_ip)
        for ip in inst_ips:
            add_swapfile(ip, worker_ram)
        f = open('hostfile', 'w')
        for ip in inst_ips:
            f.write(ip + '\n')
        f.close()



    inst_ips = check_instance_status('ips', 'all')

    f = open('host_master', 'r')
    master_ip = f.readlines()[0].split()[0]
    f.close()

    inst_ips.remove(master_ip)

    nodes_info = get_all_hosts(master_ip, inst_ips)


    if int(up) <= 6:
        hadoop_configs(nodes_info, up)

    if int(up) <= 7:
        stop_hadoop(master_ip)
        if int(up) > 0:
            py_ssh('', nodes_info[0][0], 'sudo rm -rf /tmp/*;/usr/local/hadoop/hadoop_data/hdfs/namenode/*')
            for node in nodes_info[1:]:
                py_ssh('', node[0], 'sudo rm -rf /tmp/*;sudo rm -rf /usr/local/hadoop/hadoop_data/hdfs/datanode/*')
        start_hadoop(master_ip)


    #sys.exit('We out!')
    if int(up) <= 8:
        py_ssh('', master_ip, 'sudo chown ubuntu:ubuntu /mnt')
        #py_scp_to_remote('', master_ip, 'experiment_data/covtype/covtype_0', '/mnt/covtype_0')
        #print "@#@$#$@#$ ", glob.S3CMD_CFG_PATH
        #print 's3cmd -c ' + glob.S3CMD_CFG_PATH + ' get s3://' + bucket_name + '/' + data_set + ' /mnt/' + data_set
        py_ssh('', master_ip, 's3cmd -c ' + glob.S3CMD_CFG_PATH + ' get s3://' + bucket_name + '/' + data_set + ' /mnt/' + data_set )
        py_scp_to_remote('', master_ip, 'convert_libsvm_to_csv.py', 'convert_libsvm_to_csv.py')
        py_ssh('', master_ip, 'python convert_libsvm_to_csv.py /mnt/' + data_set + ' /mnt/' + data_set + '_0')
        py_ssh('', master_ip, '/usr/local/hadoop/bin/hdfs dfs -put -f /mnt/' + data_set + '_0' + ' /; /usr/local/hadoop/bin/hdfs dfs -ls /')

        #py_ssh('', master_ip, 'source .profile; hdfs distcp s3://mybucket/myfile /root/myfile')
    if int(up) <= 9:
        spark_config(nodes_info, up)

    if int(up) <= 10:
        stop_spark(nodes_info, up)
        start_spark(nodes_info, up)

    if int(up) <= 11:
        scala_send_spark_job(nodes_info, up)

    if int(up) <= 12:
        scala_run_spark_job(nodes_info, 7077, file_name, 9000, local_jar_path[0].split('/')[-1])

    if int(up) <= 13:
        py_ssh('', master_ip, '/usr/local/hadoop/bin/hdfs dfs -put -f /mnt/scalaSVMWithSGDModel /; /usr/local/hadoop/bin/hdfs dfs -ls /')

    if int(up) <= 14:
        if int(up) > 11:
            scala_send_spark_job(nodes_info, up)
        scala_run_spark_job(nodes_info, 7077, file_name, 9000, local_jar_path[1].split('/')[-1])

########################################################################################################
########################################################################################################
########################################################################################################
########################################################################################################
########################################################################################################

def start_master_node(master_type):
    inst_ids = check_instance_status('ids', 'all')
    inst_ips = check_instance_status('ips', 'all')
    if len(inst_ips) != 0:
        terminate_instances(inst_ids, inst_ips)
    master_ip, master_id = set_up_master(master_type)
    return master_ip, master_id

def configure_experiment_machines(experiment, inst_type, master_ip, master_id, s3url):
    #Add swaps back in for bigger datasets
    inst_ids = check_instance_status('ids', 'all')
    inst_ips = check_instance_status('ips', 'all')
    inst_ips.remove(master_ip)
    inst_ids.remove(master_id)
    if len(inst_ips) != 0:
        terminate_instances(inst_ids, inst_ips)

    #add_swapfile(master_ip, master_ram)
    f = open('host_master', 'w')
    f.write(master_ip + '\n')
    f.close()

    data_set = s3url.split('/')[-1]

    py_ssh('', master_ip, 'sudo chown ubuntu:ubuntu /mnt')
    py_ssh('', master_ip, 's3cmd -c ' + glob.S3CMD_CFG_PATH + ' get ' + s3url +' /mnt/' + data_set )
    py_scp_to_remote('', master_ip, 'convert_libsvm_to_csv.py', 'convert_libsvm_to_csv.py')
    py_ssh('', master_ip, 'python convert_libsvm_to_csv.py /mnt/' + data_set + ' /mnt/' + data_set + '_0')

    #ADDS FILES NOT ADDED TO IMAGE YET
    py_scp_to_remote('', master_ip, 'scripts/get_percentage_of_file.sh', 'scripts/get_percentage_of_file.sh')


    slave_count = 2**max(map(lambda x: x[1], experiment))

    set_up_slaves(slave_count, inst_type)
    inst_ips = check_instance_status('ips', 'all')
    inst_ips.remove(master_ip)
    # for ip in inst_ips:
    #     add_swapfile(ip, worker_ram)
    f = open('hostfile', 'w')
    for ip in inst_ips:
        f.write(ip + '\n')
    f.close()


def configure_hadoop(nodes_info):

    f = open('hostfile', 'w')
    for node in nodes_info[1:]:
        f.write(node[0] + '\n')
    f.close()

    create_masters_file(nodes_info)
    create_slaves_file(nodes_info)

    create_master_hdfs_site_file()
    create_slave_hdfs_site_file()


    i = 0
    for node in nodes_info:
        create_yarn_site_file(nodes_info, i)
        create_core_site_file(nodes_info, i)

        py_scp_to_remote('', node[0], 'hostfile', 'hostfile')
        create_etc_hosts_file(nodes_info, i)
        py_scp_to_remote('', node[0], 'all_hosts_file', 'all_hosts_file')
        i += 1

    for idx in range(len(nodes_info) - 1):
        configure_slave_node(nodes_info, idx + 1)

    configure_master_node(nodes_info)


def read_job_time(master_ip):
    #remove on reimage
    py_scp_to_remote('', master_ip, 'scripts/check_if_exists.sh', 'scripts/check_if_exists.sh')

    exists = 0
    while exists == 0:
        sleep(15)
        py_ssh('', master_ip, 'source scripts/check_if_exists.sh')
        py_scp_to_local('', master_ip, 'exists', 'exists')
        g = open('exists', 'r')
        exists = int(g.readlines()[0].strip())

    py_scp_to_local('', master_ip, 'time_file', 'time_file')
    py_ssh('', master_ip, 'rm time_file')
    f = open('time_file', 'r')
    try:
        return float(f.readlines()[0].strip())
    except:
        print 'We could not read the time file.  Something happened.  Get off your lazy ass and figure it out'
        return None


def configure_and_run_experiment_frameworks(exp_or_actual, num_features, experiment, nodes_info, s3url, jar_path, algorithm):
    master_ip = nodes_info[0][0]

    stop_spark(nodes_info)
    stop_hadoop(master_ip)
    py_ssh('', master_ip, 'sudo rm -rf /tmp/*; sudo rm -rf /usr/local/hadoop/hadoop_data/hdfs/namenode/*')
    for node in nodes_info[1:]:
        py_ssh('', node[0], 'sudo rm -rf /tmp/*; sudo rm -rf /usr/local/hadoop/hadoop_data/hdfs/datanode/*')

    #Do I need this line??
    py_ssh('', master_ip, 'sudo chown ubuntu:ubuntu /tmp')

    configure_hadoop(nodes_info)
    start_hadoop(master_ip)

    data_set = s3url.split('/')[-1]

    if exp_or_actual == 'actual':
        py_ssh('', master_ip, '/usr/local/hadoop/bin/hdfs dfs -put -f /mnt/' + data_set + '_0' + ' /; /usr/local/hadoop/bin/hdfs dfs -ls /')

        spark_config(nodes_info)
        start_spark(nodes_info)

        scala_send_spark_job(nodes_info, jar_path)

        jar = jar_path.split('/')[-1]

        scala_run_spark_job(nodes_info, 7077, data_set + '_0', 9000, jar, algorithm, num_features)
    else:
        py_ssh('', master_ip, 'rm /mnt/' + data_set + '_0_exp; source scripts/get_percentage_of_file.sh ' + '/mnt/' + data_set + '_0 ' + str(experiment[0]/100.0))
        py_ssh('', master_ip, '/usr/local/hadoop/bin/hdfs dfs -put -f /mnt/' + data_set + '_0_exp' + ' /; /usr/local/hadoop/bin/hdfs dfs -ls /')

        spark_config(nodes_info)
        start_spark(nodes_info)

        scala_send_spark_job(nodes_info, jar_path)

        jar = jar_path.split('/')[-1]

        scala_run_spark_job(nodes_info, 7077, data_set + '_0_exp', 9000, jar, algorithm, num_features)

    # py_ssh('', master_ip, '/usr/local/hadoop/bin/hdfs dfs -put -f /mnt/scalaSVMWithSGDModel /; /usr/local/hadoop/bin/hdfs dfs -ls /')

    # scala_send_spark_job(nodes_info)


########################################################################################################
########################################################################################################
########################################################################################################
########################################################################################################
########################################################################################################

def main():
    run()

if __name__ == "__main__":
    main()