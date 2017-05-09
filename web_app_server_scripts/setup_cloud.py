from boto_launch_scripts import *


def get_host(ip, working_dir):
    if os.path.isfile(working_dir + '/host'):
        os.system('rm ' + working_dir + '/host')
    py_ssh('', ip, 'source scripts/get_host.sh')
    py_scp_to_local('', ip, '~/host', working_dir + '/host')

    f = open(working_dir + '/host', 'r')
    data = f.readlines()
    f.close()
    return data[0].strip(), data[1].strip()


def get_all_hosts(master_ip, ips, working_dir):
    print 'Creating Node Data Structure.'
    nodes_info = []
    master_host, master_priv_ip = get_host(master_ip, working_dir)
    nodes_info.append([master_ip, master_host, master_priv_ip])

    for ip in ips:
        host, priv_ip = get_host(ip, working_dir)
        nodes_info.append([ip, host, priv_ip])

    return nodes_info

def create_core_site_file(nodes_info, idx):
    py_ssh('', nodes_info[idx][0], 'cat hadoop_conf_files/core_site_beginning.xml host_master_tmp colon host_port_tmp hadoop_conf_files/core_site_ending.xml > hadoop_conf_files/core-site.xml')

def create_yarn_site_file(nodes_info, idx):
    port = 9000

    py_ssh('', nodes_info[idx][0], "echo -ne " + str(nodes_info[0][1]) + " > host_master_tmp; " + 
                                   "echo -ne : > colon; " + 
                                   "echo -ne " + str(port) + " > host_port_tmp")
    py_ssh('', nodes_info[idx][0], 'cat hadoop_conf_files/yarn_site_beginning.xml host_master_tmp hadoop_conf_files/yarn_site_ending.xml > hadoop_conf_files/yarn-site.xml')


def create_masters_file(nodes_info, working_dir):
    f = open(working_dir + '/masters', 'w')
    f.write(nodes_info[0][1] + '\n')
    f.close()

def create_slaves_file(nodes_info, working_dir):
    f = open(working_dir + '/slaves', 'w')
    count = 0
    for node in nodes_info[1:]:
        count += 1
        f.write(node[1] + '\n')
    f.close()

def create_etc_hosts_file(nodes_info, idx, working_dir):
    f = open(working_dir + '/hosts_file_beginning', 'w')
    f.write('127.0.0.1 localhost\n')
    f.write(nodes_info[0][2] + ' ' + nodes_info[0][1] + '\n')
    count = 0
    for node in nodes_info[1:]:
        count += 1
        f.write(node[2] + ' ' + node[1] + '\n')
    f.write('\n')
    f.close()
    os.system('cat ' + working_dir + '/hosts_file_beginning ' + working_dir + '/hosts_file_end > ' + working_dir + '/all_hosts_file')


def setup_passwordless_ssh(nodes_info, master_port):
    py_ssh('', nodes_info[0][0], 'source /home/ubuntu/scripts/master_passwordless_ssh.sh '
           + '~' + ' ' + remote_pem_path + ' ' + nodes_info[0][0] + ' ' + 
           master_port + ' ' + nodes_info[0][1] + ' ' + nodes_info[0][2])




def configure_master_node(nodes_info, working_dir):
    master_ip = nodes_info[0][0]
    py_scp_to_remote('', master_ip, working_dir + '/masters', 'masters')
    py_scp_to_remote('', master_ip, working_dir + '/slaves', 'slaves')

    py_ssh('', master_ip, 'source scripts/all_env_vars.sh')
    setup_passwordless_ssh(nodes_info, str(7077))
    py_ssh('', master_ip, 'source scripts/namenode_env_vars.sh')



def configure_slave_node(nodes_info, idx):
    ip = nodes_info[idx][0]
    py_ssh('', ip, 'source scripts/all_env_vars.sh')
    py_ssh('', ip, 'source scripts/datanode_env_vars.sh ' + nodes_info[0][2] + ' ' + nodes_info[0][1])


def spark_config(nodes_info, working_dir):
    f = open(working_dir + '/spark_slaves', 'w')
    for node in nodes_info[1:]:
        f.write(node[0] + '\n')
    f.close()

    py_scp_to_remote('', nodes_info[0][0], working_dir + '/spark_slaves', 'spark-2.0.0/conf/slaves')
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

def scala_run_spark_job(nodes_info, worker_type, master_port, file_name, hadoop_master_port, jar, num_features, iterations, log_path):
    #NON-YARN
    driver_mem = ''
    executor_mem = ''
    spark_max_result = ''
    num_machs = len(nodes_info[1:])
    num_cores = 4*num_machs
    num_partitions = num_cores


    for node in nodes_info:
        py_scp_to_remote('', node[0], 'spark_conf_files/log4j.properties', '~/spark-2.0.0/conf/log4j.properties')
        py_scp_to_remote('', node[0], 'spark_conf_files/hadoop-log4j.properties', '~/log4j.properties')
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


    run_str = ['~/spark-2.0.0/bin/spark-submit --verbose --master ' + 
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


def create_hdfs_site_file(nodes_info, replication, tempdir_path, idx):

    py_ssh('', nodes_info[idx][0], "echo -ne " + str(replication) + " > replication_tmp; " + 
                                   "echo -ne file://" + tempdir_path + "/namenode > namenode_path; " + 
                                   "echo -ne file://" + tempdir_path + "/datanode > datanode_path")
    cat_arg = ['cat hadoop_conf_files/hdfs_site_beginning.xml replication_tmp ' + 
               'hadoop_conf_files/hdfs_site_middle_1.xml namenode_path ' + 
               'hadoop_conf_files/hdfs_site_middle_1.xml datanode_path ' + 
               'hadoop_conf_files/hdfs_site_ending.xml ' + 
               '> hadoop_conf_files/hdfs-site.xml']
    py_ssh('', nodes_info[idx][0], cat_arg[0])


def configure_hadoop(nodes_info, replication, working_dir):

    #PARAMS FOR HADOOP
    tempdir_path = '/mnt'
    num_machs = len(nodes_info) - 1

    if num_machs < replication:
        replication = num_machs


    f = open(working_dir + '/hostfile', 'w')
    for node in nodes_info[1:]:
        f.write(node[0] + '\n')
    f.close()

    preconfigure_nodes(nodes_info, working_dir)

    for i, node in enumerate(nodes_info):

        py_ssh('', node[0], 'sudo chown ubuntu:ubuntu /')
        create_yarn_site_file(nodes_info, i)
        create_core_site_file(nodes_info, i)
        create_hdfs_site_file(nodes_info, replication, tempdir_path, i)

        py_scp_to_remote('', node[0], working_dir + '/hostfile', 'hostfile')
        create_etc_hosts_file(nodes_info, i, working_dir)
        py_scp_to_remote('', node[0], working_dir + '/all_hosts_file', 'all_hosts_file')





def configure_nodes(nodes_info, working_dir):

    for idx in range(len(nodes_info) - 1):
        configure_slave_node(nodes_info, idx + 1)

    configure_master_node(nodes_info, working_dir)

def preconfigure_nodes(nodes_info, working_dir):
    create_masters_file(nodes_info, working_dir)
    create_slaves_file(nodes_info, working_dir)




def read_job_time(nodes_info, working_dir):

    master_ip = nodes_info[0][0]

    exists = 0
    count = 0
    while exists == 0:
        sleep(15)
        py_ssh('', master_ip, 'source scripts/check_if_exists.sh')
        py_scp_to_local('', master_ip, 'exists', working_dir + '/exists')
        g = open(working_dir + '/exists', 'r')
        exists = int(g.readlines()[0].strip())
        count += 1
        if count > 10:
            print "There was an error in the log file.  Check it."
            return None

    py_scp_to_local('', master_ip, 'time_file', working_dir + '/time_file')
    py_ssh('', master_ip, 'rm ' + working_dir + '/time_file')
    f = open(working_dir + '/time_file', 'r')
    try:
        return map(lambda x: float(x.strip()), f.readlines())
    except:
        print 'We could not read the time file.  Something happened.  Get off your lazy ass and figure it out'
        return None


def get_dataset(s3url, master_ip, working_dir):
    dataset = s3url.split('/')[-1]
    py_ssh('', master_ip, 'sudo chown ubuntu:ubuntu /mnt; mkdir namenode')
    get_data = 's3cmd -d -v -c ' + remote_s3cfg + ' get ' + s3url + '_0' +' /mnt/' + dataset + '_0'

    flag = 0
    while(1):
        py_ssh_to_log('', master_ip, 'sudo chown ubuntu:ubuntu /mnt; ' +  get_data, working_dir + '/s3_log')
        sleep(3)
        f = open(working_dir + '/s3_log', 'r')
        if '403' not in f.read():
            break
        if flag == 0:
            print 'Waiting in 403 Forbidden Loop.'
        flag = 1
        os.system('rm ' + working_dir + '/s3_log')





def configure_machines_for_spark_job_experiments(s3url, working_dir, replication, instance_ips):

    dataset = s3url.split('/')[-1]
    log_dir = working_dir + '/profile_logs'

    master_ip = instance_ips[0]
    f = open(working_dir + '/host_master', 'w')
    f.write(master_ip + '\n')
    f.close()

    ips = instance_ips[1:]
    nodes_info = get_all_hosts(master_ip, ips, working_dir)
    os.system('cp temp_files/hosts_file_end ' + working_dir + '/')


    ###
    #remove on reimage
    #os.system('tar -czf scripts_to_run_remotely.tar.gz scripts_to_run_remotely')
    #os.system('mv scripts_to_run_remotely.tar.gz temp_files/')
    for node in nodes_info:
        py_ssh('', node[0], 'mv hadoop_xml_files hadoop_conf_files')
        py_scp_to_remote('', node[0], 'scripts_to_run_remotely/all_env_vars.sh', '~/scripts/all_env_vars.sh')
    for ip in ips:
        py_ssh('', ip, 'sudo chown ubuntu:ubuntu /mnt;mkdir /mnt/datanode; mkdir /mnt/spark')
    py_ssh('', master_ip, 'sudo chown ubuntu:ubuntu /mnt; mkdir /mnt/spark')
        #py_ssh('', node[0], 'tar -xzf scripts_to_run_remotely.tar.gz; rm -rf scripts; mv scripts_to_run_remotely scripts')


    get_dataset(s3url, master_ip, working_dir)

    configure_hadoop(nodes_info, replication, working_dir)
    configure_nodes(nodes_info, working_dir)
    spark_config(nodes_info, working_dir)
    py_ssh('', master_ip, 'source scripts/restart_hadoop.sh')
    start_spark(nodes_info)


    return dataset, nodes_info



def create_hdfs_file(dataset, experiment, master_ip, first, second):
    if not first:
        if not second:
            py_ssh('', master_ip, 'mv /mnt/' + dataset + '_0_exp /mnt/' + dataset + '_0')

    py_ssh('', master_ip, 'python scripts/get_percentage_of_file.py ' + '/mnt/' + dataset + '_0 ' + str(experiment) + ' ' + str(True))
    py_ssh('', master_ip, '/usr/local/hadoop/bin/hdfs dfs -put /mnt/' + dataset + '_0_exp ' + ' /; /usr/local/hadoop/bin/hdfs dfs -ls /')

def clean_up_experiment(dataset, master_ip):
    stop_spark(nodes_info)
    py_ssh('', master_ip, '/usr/local/hadoop/bin/hdfs dfs -rm /' + dataset + '_0_exp ' + '; /usr/local/hadoop/bin/hdfs dfs -ls /')


def run_spark_experiment(experiment, dataset, nodes_info, worker_type, jar_path, num_features, iterations, log_path):
    scala_send_spark_job(nodes_info, jar_path)
    jar = jar_path.split('/')[-1]
    scala_run_spark_job(nodes_info, worker_type, 7077, dataset + '_0_exp ', 9000, jar, num_features, iterations, log_path)

