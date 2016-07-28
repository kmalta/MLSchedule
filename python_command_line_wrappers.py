import os
import sys
from subprocess import Popen, PIPE
from time import sleep

#Dependency Files
import global_file


def py_scp_to_remote(additional_options, ip, file1, file2):
    ao = additional_options

    cmd = 'scp ' + ao + ' ' + OPTIONS + ' ' + file1 + ' ubuntu@' + ip + ':' + file2
    proc = Popen(cmd, shell=True, executable='/bin/bash')
    proc.wait()
    return proc


def py_scp_to_local(additional_options, ip, file1, file2):
    ao = additional_options

    cmd = 'scp ' + ao + ' ' + OPTIONS + ' ubuntu@' + ip + ':' + file1 + ' ' + file2
    proc = Popen(cmd, shell=True, executable='/bin/bash')
    proc.wait()
    return proc


def py_ssh(additional_options, ip, post_command):
    pc = post_command
    ao = additional_options

    cmd = 'ssh ' + ao + ' ' + OPTIONS + ' ubuntu@' + ip + ' "' + pc + '"'
    proc = Popen(cmd, shell=True, executable='/bin/bash')
    proc.wait()
    return proc


def py_wait_proc(process_str):
    cmd = [process_str]
    proc = Popen(cmd.split(), shell=True, executable='/bin/bash')
    proc.wait()
    return proc


def py_out_proc(process_str):
    cmd = [process_str]
    (stdout, stderr) = Popen(cmd.split(), stdout=PIPE).communicate()
    return stdout


def py_cmd_line(process_str):
    os.system(process_str)
    return

