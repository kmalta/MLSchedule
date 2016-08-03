#!/usr/bin/env python
import os
from os.path import dirname, join
import time
import sys


#Command Line Parameters
clp = sys.argv

epochs = int(clp[1])
cores = int(clp[2])
staleness = int(clp[3])
data_path = clp[4]
use_weight_file = clp[5]
pem_path = clp[6]

data_set_path = data_path + "/train_file"


hostfile_name = "hostfile_petuum_format"

app_dir = dirname(dirname(os.path.realpath(__file__)))
proj_dir = dirname(dirname(app_dir))

hostfile = join(proj_dir, "machinefiles", hostfile_name)

ssh_cmd = (
    "ssh "
    "-i " + pem_path + " "
    "-o stricthostkeychecking=no "
    "-o UserKnownHostsFile=/dev/null "
    )



params = {
    "train_file": join(app_dir, data_set_path)
    , "test_file": join(app_dir, data_set_path)
    , "global_data": "false"
    , "perform_test": "false"
    , "use_weight_file": use_weight_file
    , "weight_file": ""
    , "num_epochs": epochs
    , "num_batches_per_epoch": 10
    , "init_lr": 0.01 # initial learning rate
    , "lr_decay_rate": 0.99 # lr = init_lr * (lr_decay_rate)^T
    , "num_batches_per_eval": 10
    , "num_train_eval": 10000 # compute train error on these many train.
    , "num_test_eval": 20
    , "lambda": 0
    , "output_file_prefix": join(app_dir, "out")
    }

petuum_params = {
    "hostfile": hostfile
    , "num_app_threads": cores
    , "staleness": staleness
    , "num_comm_channels_per_client": 1 # 1~2 are usually enough.
    }

prog_name = "mlr_main"
prog_path = join(app_dir, "bin", prog_name)

env_params = (
  "GLOG_logtostderr=true "
  "GLOG_v=-1 "
  "GLOG_minloglevel=0 "
  )

# Get host IPs
with open(hostfile, "r") as f:
  hostlines = f.read().splitlines()
host_ips = [line.split()[1] for line in hostlines]
petuum_params["num_clients"] = len(host_ips)

# os.system is synchronous call.
os.system("killall -q " + prog_name)
print "Done killing"

if not params["output_file_prefix"].startswith("hdfs://"):
  os.system("mkdir -p " + join(app_dir, "output"))

for client_id, ip in enumerate(host_ips):
  petuum_params["client_id"] = client_id
  cmd = ssh_cmd + ip + " "
  #cmd += "export CLASSPATH=`hadoop classpath --glob`:$CLASSPATH; "
  cmd += env_params + " " + prog_path
  cmd += "".join([" --%s=%s" % (k,v) for k,v in petuum_params.items()])
  cmd += "".join([" --%s=%s" % (k,v) for k,v in params.items()])
  cmd += " &"
  print cmd
  os.system(cmd)

  if client_id == 0:
    print "Waiting for first client to set up"
    time.sleep(2)
