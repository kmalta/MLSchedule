import os, sys
from subprocess import Popen, PIPE
from time import sleep

from web_app_server_scripts.cloud_configs.aristotle_cfg import *

user = 'ubuntu'


def py_scp_to_remote(additional_options, ip, file1, file2):
    ao = additional_options

    cmd = 'scp ' + ao + ' ' + options + ' ' + file1 + ' ' + user + '@' + ip + ':' + file2
    proc = Popen(cmd, shell=True, executable='/bin/bash')
    proc.wait()
    return proc


def py_scp_to_local(additional_options, ip, file1, file2):
    ao = additional_options

    cmd = 'scp ' + ao + ' ' + options + '  ' + user + '@' + ip + ':' + file1 + ' ' + file2
    proc = Popen(cmd, shell=True, executable='/bin/bash')
    proc.wait()
    return proc


def py_ssh(additional_options, ip, post_command):
    pc = post_command
    ao = additional_options

    cmd = 'ssh ' + ao + ' ' + options + '  ' + user + '@' + ip + ' "' + pc + '"'
    proc = Popen(cmd, shell=True, executable='/bin/bash')
    proc.wait()
    return proc

def py_ssh_to_log(additional_options, ip, post_command, log_path):
    pc = post_command
    ao = additional_options
    log = open(log_path, 'a')

    cmd = 'ssh ' + ao + ' ' + options + '  ' + user + '@' + ip + ' "' + pc + '"'
    proc = Popen(cmd, stdout=log, stderr=log, shell=True, executable='/bin/bash')
    proc.wait()
    return proc

def py_wait_proc(process_str):
    proc = Popen([process_str], shell=True, executable='/bin/bash')
    proc.wait()
    return proc


def py_out_proc(process_str):
    (stdout, stderr) = Popen(process_str.split(), stdout=PIPE).communicate()
    return stdout


def py_cmd_line(process_str):
    os.system(process_str)
    return

