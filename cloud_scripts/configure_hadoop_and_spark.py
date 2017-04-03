import glob
from deploy_cloud import *

import sys
from time import time
import os


import boto
from boto.ec2.regioninfo import RegionInfo
import boto.s3.connection






def set_up_master(master_type):
    master_ip, master_id = launch_instance_with_metadata(master_type)
    return master_ip, master_id


def set_up_slaves(num_slaves, slave_types):
    launch_instances(num_slaves, slave_types)


def get_host(ip):
    py_ssh('', ip, 'source scripts/get_host.sh')
    py_scp_to_local('', ip, '/home/ubuntu/host', '~/MLSchedule/temp_files/host')
    f = open('temp_files/host', 'r')
    data = f.readlines()
    f.close()
    return data[0].strip(), data[1].strip()


def get_all_hosts(master_ip, ips):
    print "getting all hosts!"
    nodes_info = []
    master_host, master_priv_ip = get_host(master_ip)
    nodes_info.append([master_ip, master_host, master_priv_ip])
    ips.remove(master_ip)
    for ip in ips:
        host, priv_ip = get_host(ip)
        nodes_info.append([ip, host, priv_ip])
        #remove_on_reimage

    print repr(nodes_info)
    return nodes_info

def create_core_site_file(nodes_info, idx):
    py_ssh('', nodes_info[idx][0], 'cat hadoop_conf_files/core_site_beginning.xml host_master_tmp colon host_port_tmp hadoop_conf_files/core_site_ending.xml > hadoop_conf_files/core-site.xml')

def create_yarn_site_file(nodes_info, idx):
    port = 9000


    py_ssh('', nodes_info[idx][0], "echo -ne " + str(nodes_info[0][1]) + " > host_master_tmp; " + 
                                   "echo -ne : > colon; " + 
                                   "echo -ne " + str(port) + " > host_port_tmp")
    py_ssh('', nodes_info[idx][0], 'cat hadoop_conf_files/yarn_site_beginning.xml host_master_tmp hadoop_conf_files/yarn_site_ending.xml > hadoop_conf_files/yarn-site.xml')


def create_masters_file(nodes_info):
    f = open('temp_files/masters', 'w')
    f.write(nodes_info[0][1] + '\n')
    f.close()

def create_slaves_file(nodes_info):
    f = open('temp_files/slaves', 'w')
    count = 0
    for node in nodes_info[1:]:
        count += 1
        f.write(node[1] + '\n')
    f.close()

def create_etc_hosts_file(nodes_info, idx):
    print "making hosts files!!"
    f = open('temp_files/hosts_file_beginning', 'w')
    f.write('127.0.0.1 localhost\n')
    f.write(nodes_info[0][2] + ' ' + nodes_info[0][1] + '\n')
    count = 0
    for node in nodes_info[1:]:
        count += 1
        f.write(node[2] + ' ' + node[1] + '\n')
    f.write('\n')
    f.close()
    os.system('cat temp_files/hosts_file_beginning temp_files/hosts_file_end > temp_files/all_hosts_file')


def setup_passwordless_ssh(nodes_info, master_port):
    py_ssh('', nodes_info[0][0], 'source /home/ubuntu/scripts/master_passwordless_ssh.sh '
           + glob.REMOTE_PATH + ' ' + glob.REMOTE_PEM_PATH + ' ' + nodes_info[0][0] + ' ' + 
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
            py_scp_to_remote('', node[0], 'temp_files/hostfile', 'hostfile')
            create_etc_hosts_file(nodes_info, i)
            py_scp_to_remote('', node[0], 'temp_files/all_hosts_file', 'all_hosts_file')
            i += 1


    if int(up) <= 4:
        for idx in range(len(nodes_info) - 1):
            configure_slave_node(nodes_info, idx + 1)

    if int(up) <= 5:
        configure_master_node(nodes_info)




def configure_master_node(nodes_info):
    master_ip = nodes_info[0][0]

    py_scp_to_remote('', master_ip, 'temp_files/masters', 'masters')
    py_scp_to_remote('', master_ip, 'temp_files/slaves', 'slaves')

    #REMOVE ON REIMAGE
    py_scp_to_remote('', master_ip, 'scripts_to_run_remotely/get_percentage_of_file.py', 'scripts/get_percentage_of_file.py')

    py_ssh('', master_ip, 'source scripts/all_env_vars.sh')

    setup_passwordless_ssh(nodes_info, str(7077))

    py_ssh('', master_ip, 'source scripts/namenode_env_vars.sh')

def configure_slave_node(nodes_info, idx):
    ip = nodes_info[idx][0]


    py_ssh('', ip, 'source scripts/all_env_vars.sh')
    py_ssh('', ip, 'source scripts/datanode_env_vars.sh ' + nodes_info[0][2] + ' ' + nodes_info[0][1])




def spark_config(nodes_info):
    f = open('temp_files/spark_slaves', 'w')
    for node in nodes_info[1:]:
        f.write(node[0] + '\n')
    f.close()

    print "wrote conf/slaves file"

    py_scp_to_remote('', nodes_info[0][0], 'temp_files/spark_slaves', 'spark-2.0.0/conf/slaves')
    for node in nodes_info:
        #REMOVE ON REIMAGE
        py_ssh('', node[0], 'echo -e export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop >> /home/ubuntu/spark-2.0.0/conf/spark-env.sh')
        py_ssh('', node[0], 'echo -e export SPARK_LOCAL_DIRS=/mnt/spark >> /home/ubuntu/spark-2.0.0/conf/spark-env.sh')


def start_spark(nodes_info):
    py_ssh('', nodes_info[0][0], 'spark-2.0.0/sbin/start-all.sh')

def stop_spark(nodes_info):

    py_ssh('', nodes_info[0][0], 'spark-2.0.0/sbin/stop-slaves.sh')
    py_ssh('', nodes_info[0][0], 'spark-2.0.0/sbin/stop-master.sh')

def scala_send_spark_job(nodes_info, jar_path):
    for node in nodes_info:
        py_scp_to_remote('', node[0], jar_path, 'jars/' + jar_path.split('/')[-1])

def scala_run_spark_job(nodes_info, worker_type, master_port, file_name, hadoop_master_port, jar, algorithm, num_features, iterations, log_path):
    #NON-YARN
    driver_mem = ''
    executor_mem = ''
    spark_max_result = ''
    num_machs = len(nodes_info[1:])
    num_cores = 4*num_machs
    num_partitions = num_cores

    print file_name

    for node in nodes_info:
        py_scp_to_remote('', node[0], '/Users/Kevin/MLSchedule/spark_conf_files/log4j.properties', '/home/ubuntu/spark-2.0.0/conf/log4j.properties')
        py_scp_to_remote('', node[0], '/Users/Kevin/MLSchedule/spark_conf_files/hadoop-log4j.properties', '~/log4j.properties')
        py_ssh('', node[0], 'sudo mv ~/log4j.properties /usr/local/hadoop/etc/hadoop/log4j.properties')


    #speculation_flag = ' --conf spark.speculation=true '
    speculation_flag = ''
    local_dir = ' --conf spark.local.dir=/mnt/spark/ '


    if 'hi1.4xlarge' in worker_type:
        driver_mem = '35000m'
        executor_mem = '35000m'
        spark_max_result = ' --conf spark.driver.maxResultSize=7g'
        os.system('cp spark_conf_files/hi1.4xlarge.conf spark_conf_files/spark-defaults.conf')
        num_cores = 2*num_cores
    elif 'hs1.8xlarge' in worker_type:
        driver_mem = '50000m'
        executor_mem = '50000m'
        spark_max_result = ' --conf spark.driver.maxResultSize=10g'
        os.system('cp spark_conf_files/hs1.8xlarge.conf spark_conf_files/spark-defaults.conf')
        num_cores = 2*num_cores
    elif 'm1.large' in worker_type:
        driver_mem = '11500m'
        executor_mem = '11500m'
        spark_max_result = ' --conf spark.driver.maxResultSize=8g'
        os.system('cp spark_conf_files/m1.large.conf spark_conf_files/spark-defaults.conf')
    else:
        driver_mem = '4000m'
        executor_mem = '4000m'
        os.system('cp spark_conf_files/cg1.4xlarge.conf spark_conf_files/spark-defaults.conf')

    for node in nodes_info:
        py_scp_to_remote('', node[0], 'spark_conf_files/spark-defaults.conf', '~/spark-2.0.0/conf/spark-defaults.conf')


    run_str = ['/home/ubuntu/spark-2.0.0/bin/spark-submit --verbose --master ' + 
               'spark://' + nodes_info[0][1] + ':7077' + ' --deploy-mode client ' + 
               ' --driver-memory ' + driver_mem + ' --executor-memory ' + executor_mem + 
               ' --conf spark.shuffle.spill=false' + spark_max_result + 
               ' --num-executors ' + str(num_machs) + 
               ' --conf spark.default.parallelism=' + str(num_cores) + 
               speculation_flag + local_dir + 
               #'--conf spark.storage.memoryFraction'
               ' file:///home/ubuntu/jars/'+ jar + ' cluster ' + 
               str(num_features) + ' ' + nodes_info[0][1] + ' ' + str(hadoop_master_port) + 
               ' ' + file_name + ' ' + str(iterations) + ' ' + str(num_partitions)]


    print "@@@@@@@@@@@@@@@" + run_str[0]
    py_ssh_to_log('', nodes_info[0][0], run_str[0], log_path)



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
    py_ssh('', master_ip, 'mv hadoop_xml_files hadoop_conf_files')
    py_scp_to_remote('', master_ip, 'scripts_to_run_remotely/all_env_vars.sh', 'scripts/all_env_vars.sh')
    return master_ip, master_id

def configure_experiment_machines(experiment, inst_type, master_ip, master_id, s3url):
    #Add swaps back in for bigger datasets
    inst_ids = check_instance_status('ids', 'all')
    inst_ips = check_instance_status('ips', 'all')
    inst_ips.remove(master_ip)
    inst_ids.remove(master_id)
    if len(inst_ips) != 0:
        terminate_instances(inst_ids, inst_ips)

    f = open('temp_files/host_master', 'w')
    f.write(master_ip + '\n')
    f.close()

    data_set = s3url.split('/')[-1]

    py_ssh('', master_ip, 'sudo chown ubuntu:ubuntu /mnt; mkdir namenode')
    get_data = 's3cmd -d -v -c ' + glob.REMOTE_CFG + ' get ' + s3url + '_0' +' /mnt/' + data_set + '_0'

    while(1):

        os.system('rm temp_files/s3_log')
        py_ssh_to_log('', master_ip, 'sudo chown ubuntu:ubuntu /mnt; ' +  get_data, 'temp_files/s3_log')
        sleep(3)
        f = open('temp_files/s3_log', 'r')
        if '403' not in f.read():
            break
        print "Stuck in 403 Forbidden Loop"

    #remove on reimage
    py_scp_to_remote('', master_ip, 'scripts_to_run_remotely/usage.sh', 'scripts/usage.sh')



    slave_count = [x[1] for x in experiment][0]


    set_up_slaves(slave_count, inst_type)
    inst_ips = check_instance_status('ips', 'all')
    inst_ips.remove(master_ip)
    # for ip in inst_ips:
    #     add_swapfile(ip, worker_ram)
    f = open('temp_files/hostfile', 'w')
    for ip in inst_ips:
        py_ssh('', ip, 'mv hadoop_xml_files hadoop_conf_files')
        py_scp_to_remote('', ip, 'scripts_to_run_remotely/all_env_vars.sh', 'scripts/all_env_vars.sh')
        f.write(ip + '\n')
        py_ssh('', ip, 'sudo chown ubuntu:ubuntu /mnt;mkdir /mnt/datanode; mkdir /mnt/spark')
    py_ssh('', master_ip, 'sudo chown ubuntu:ubuntu /mnt; mkdir /mnt/spark')
    f.close()

def assert_datanodes_liveness(nodes_info):
    nodes_are_alive = 0
    flag = 0

    count = 0
    while nodes_are_alive == 0:
        py_ssh('', nodes_info[0][0], 'source scripts/restart_hadoop.sh ; source scripts/check_hdfs_cluster_status.sh')
        py_scp_to_local('', nodes_info[0][0], 'dfsadmin_report', 'temp_files/dfsadmin_report')
        f = open('temp_files/dfsadmin_report', 'r')
        for line in f:
            if 'Live datanodes' in line:
                live_nodes = int(line.split()[2].split(')')[0].split('(')[1])
                if live_nodes == len(nodes_info) - 1:
                    nodes_are_alive = 1
                    break
                else:
                    break
        count += 1
        if count > 10:
            sys.exit("Data node liveness fail")

def create_hdfs_site_file(nodes_info, replication, tempdir_path, idx):

    py_ssh('', nodes_info[idx][0], "echo -ne " + str(replication) + " > replication_tmp; " + 
                                   "echo -ne file://" + tempdir_path + "/namenode > namenode_path; " + 
                                   "echo -ne file://" + tempdir_path + "/datanode > datanode_path")
    cat_arg = ['cat hadoop_conf_files/hdfs_site_beginning.xml replication_tmp ' + 
               'hadoop_conf_files/hdfs_site_middle_1.xml namenode_path ' + 
               'hadoop_conf_files/hdfs_site_middle_1.xml datanode_path hadoop_conf_files/hdfs_site_ending.xml ' + 
               '> hadoop_conf_files/hdfs-site.xml']
    py_ssh('', nodes_info[idx][0], cat_arg[0])


def configure_hadoop(nodes_info, replication, experiment):

    #PARAMS FOR HADOOP
    tempdir_path = '/mnt'

    machs = 2**experiment[1]

    junk_chunks = 1
    if machs < replication:
        junk_chunks = float(replication - machs)/machs
        replication = machs

    f = open('temp_files/hostfile', 'w')
    for node in nodes_info[1:]:
        f.write(node[0] + '\n')
    f.close()

    create_masters_file(nodes_info)
    create_slaves_file(nodes_info)


    i = 0
    for node in nodes_info:

        py_ssh('', node[0], 'sudo chown ubuntu:ubuntu /')
        py_scp_to_remote('', node[0], 'scripts_to_run_remotely/usage.sh', 'scripts/usage.sh')
        create_yarn_site_file(nodes_info, i)
        create_core_site_file(nodes_info, i)
        create_hdfs_site_file(nodes_info, replication, tempdir_path, i)

        py_scp_to_remote('', node[0], 'temp_files/hostfile', 'hostfile')
        create_etc_hosts_file(nodes_info, i)
        py_scp_to_remote('', node[0], 'temp_files/all_hosts_file', 'all_hosts_file')

        i += 1

    for idx in range(len(nodes_info) - 1):
        configure_slave_node(nodes_info, idx + 1)

    configure_master_node(nodes_info)
    return junk_chunks


def read_job_time(master_ip):

    exists = 0
    count = 0
    while exists == 0:
        sleep(15)
        py_ssh('', master_ip, 'source scripts/check_if_exists.sh')
        py_scp_to_local('', master_ip, 'exists', 'temp_files/exists')
        g = open('temp_files/exists', 'r')
        exists = int(g.readlines()[0].strip())
        count += 1
        if count > 10:
            print "There was an error in the log file.  Check it."
            return None

    py_scp_to_local('', master_ip, 'time_file', 'temp_files/time_file')
    py_ssh('', master_ip, 'rm temp_files/time_file')
    f = open('temp_files/time_file', 'r')
    try:
        return map(lambda x: float(x.strip()), f.readlines())
    except:
        print 'We could not read the time file.  Something happened.  Get off your lazy ass and figure it out'
        return None


def configure_and_run_experiment_frameworks(exp_or_actual, num_features, experiment, nodes_info, s3url, jar_path, algorithm, iterations, first, prev_exp_percent, replication, log_path, worker_type):
    master_ip = nodes_info[0][0]

    stop_spark(nodes_info)

    data_set = s3url.split('/')[-1]

    suffix = ''

    print "FIRST:", repr(first)
    if first == True:
        junk_chunks = configure_hadoop(nodes_info, replication, experiment)
        assert_datanodes_liveness(nodes_info)

        #Do I need this line??
        py_ssh('', master_ip, 'sudo chown ubuntu:ubuntu /tmp')



        if experiment[0] == 0:
            py_ssh('', master_ip, 'mv /mnt/' + data_set + '_0 /mnt/' + data_set + '_0_exp')
            py_ssh('', master_ip, 'rm /mnt/' + data_set + '_0_exp; python scripts/get_percentage_of_file.py ' + '/mnt/' + data_set + '_0 ' + str(99.9/100.0) + ' ' + str(False))
            py_ssh('', master_ip, '/usr/local/hadoop/bin/hdfs dfs -put -f /mnt/' + data_set + '_0_exp ' + ' /; /usr/local/hadoop/bin/hdfs dfs -ls /')
        else:
            py_ssh('', master_ip, '/usr/local/hadoop/bin/hdfs dfs -rm -f /' + data_set + '_0_exp; /usr/local/hadoop/bin/hdfs dfs -ls /')
            py_ssh('', master_ip, 'rm /mnt/' + data_set + '_0_exp; python scripts/get_percentage_of_file.py ' + '/mnt/' + data_set + '_0 ' + str(experiment[0]) + ' ' + str(True))
            py_ssh('', master_ip, '/usr/local/hadoop/bin/hdfs dfs -put -f /mnt/' + data_set + '_0_exp ' + ' /; /usr/local/hadoop/bin/hdfs dfs -ls /')




        suffix = '_0_exp '
    else:
        suffix = '_0_exp '


    spark_config(nodes_info)
    start_spark(nodes_info)

    scala_send_spark_job(nodes_info, jar_path)

    jar = jar_path.split('/')[-1]

    scala_run_spark_job(nodes_info, worker_type, 7077, data_set + suffix, 9000, jar, algorithm, num_features, iterations, log_path)




########################################################################################################
########################################################################################################
########################################################################################################
########################################################################################################
########################################################################################################

def main():
    run()

if __name__ == "__main__":
    main()