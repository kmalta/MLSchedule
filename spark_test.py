import boto
from boto.ec2.regioninfo import RegionInfo
import boto.s3.connection

import sys
from time import time
import os

from pyspark import SparkContext

from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils

access_key="AKIAAXGNAZPKGAEVH5TW"
secret_key="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"
# ec2_host="169.231.234.30"
# s3_host="169.231.234.30"
bucket_name='covtype-data'
data_set='covtype'
region='race.cs.ucsb.edu'
num_features=784
num_classes=10
master_type='m3.2xlarge'
worker_type='m3.xlarge'


def classes_num(value):
    if data_set == 'mnist':
        return value
    else:
        return value - 1


def parsePoint(line):
    values = line.split(' ')
    features = map(lambda x: [int(x.split(':')[0]) - 1, float(x.split(':')[1])], values[1:])
    ret_val = LabeledPoint(classes_num(int(values[0])), SparseVector(num_features, dict(features)))
    return ret_val


def map_func(key):
    print str(os.getpid()) + "        " + str(key._get_key())
    for line in key.get_contents_as_string().splitlines():
        yield parsePoint(line)






args = sys.argv
host_name=args[0]
host_port=args[1]
file_name=args[2]

file_path = '/Users/Kevin/MLSchedule/experiment_data/covtype/covtype'
#file_path="hdfs://" + host_name +":" + host_port + "/" + file_name


sc = SparkContext(appName="PythonLogisticRegressionWithLBFGSExample")


data = MLUtils.loadLibSVMFile(sc, file_path)

# Build the model
model = LogisticRegressionWithLBFGS.train(data, iterations=100)

# Evaluating the model on training data
labelsAndPreds = data.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(data.count())
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Training Error = " + str(trainErr))

# Save and load model
model.save(sc, "target/tmp/pythonLogisticRegressionWithLBFGSModel")
sameModel = LogisticRegressionModel.load(sc,
                                         "target/tmp/pythonLogisticRegressionWithLBFGSModel")








