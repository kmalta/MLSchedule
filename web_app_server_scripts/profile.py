from glob import *
from scipy.optimize import nnls
import numpy as np
import math
import cvxopt as cvx
import picos as pic
import sys
from time import sleep, gmtime, strftime, time
import os
import ast
import json

from configure_hadoop_and_spark import *


#This spins up all the machines, but terminates the slowest %25 of the machine count
def test_machines():
    1

def determine_profile_experiments(json_dict):
    number_of_machines_to_profile = json_dict['machine count'] + int(json_dict['machine count']*.25) + 1
    test_machines()
    




def main():
    1



if __name__ == "__main__":
    main()
