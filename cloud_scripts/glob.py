def set_globals():
    LOC_PREF = 'cloud_configs/'
    f = open(LOC_PREF + 'script-cfg', 'r')
    data = f.readlines()
    global CLOUD
    CLOUD = data[0].split()[2]
    global KEY
    KEY = CLOUD + '-key'
    global KEY_STR
    KEY_STR = '--key ' + KEY
    global LOC_PEM_PATH
    LOC_PEM_PATH = LOC_PREF + CLOUD + '/' + KEY + '.pem'
    global REMOTE_PEM_PATH
    REMOTE_PEM_PATH = CLOUD + '/' + KEY + '.pem'
    global OPTIONS
    OPTIONS = ' '.join(data[1].split()[2:]).strip() + ' ' + LOC_PEM_PATH
    global REMOTE_PATH
    REMOTE_PATH = data[2].split()[2].strip()
    global BUCKET
    BUCKET = CLOUD + '-' + data[3].split()[2].strip()
    global BUCKET_STR
    BUCKET_STR = '--bucket ' + BUCKET
    global PREFIX_SUFFIX
    PREFIX_SUFFIX = data[4].split()[2].strip()
    global VIRT_TYPE
    VIRT_TYPE = data[5].split()[2].strip()
    global VIRT_TYPE_STR
    VIRT_TYPE_STR = '--virtualization-type ' + VIRT_TYPE
    global DATA_PATH
    DATA_PATH = data[6].split()[2].strip() + '/data_loc'
    global DATA_SET
    DATA_SET = data[7].split()[2].strip()
    global DATA_SET_BUCKET
    DATA_SET_BUCKET = data[8].split()[2].strip()
    global DATA_SET_PATH
    DATA_SET_PATH = data[9].split()[2].strip() + '_data' + '/' + DATA_SET

    f2 = open(LOC_PREF + CLOUD + '/' + CLOUD + '-params', 'r')
    data2 = f2.readlines()
    global BASE_IP
    BASE_IP = data2[0].split()[2].strip()
    global REGION
    REGION = data2[1].split()[2].strip()
    global REGION_STR
    REGION_STR = '--region ' + REGION
    global SECURITY_GROUP
    SECURITY_GROUP = data2[2].split()[2].strip()
    global SECURITY_GROUP_STR
    SECURITY_GROUP_STR = '--group ' + SECURITY_GROUP
    global BASE_IMAGE
    BASE_IMAGE = data2[3].split()[2].strip()
    global LAUNCH_FROM
    LAUNCH_FROM = data2[4].split()[2].strip()

    global S3CMD_CFG_PATH
    S3CMD_CFG_PATH = LOC_PREF + CLOUD + '/' + CLOUD + '-s3cfg'
    global REMOTE_CFG
    REMOTE_CFG = REMOTE_PATH + '/' + CLOUD + '/' + CLOUD + '-s3cfg'
