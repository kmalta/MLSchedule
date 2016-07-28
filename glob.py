def set_globals():
    f = open('./script-cfg', 'r')
    data = f.readlines()
    global CLOUD
    CLOUD = data[0].split()[2]
    global KEY
    KEY = CLOUD + '-key'
    global KEY_STR
    KEY_STR = '--key ' + KEY
    global PEM_PATH
    PEM_PATH = CLOUD + '/' + KEY + '.pem'
    global OPTIONS
    OPTIONS = ' '.join(data[1].split()[2:]) + ' ' + PEM_PATH
    global REMOTE_PATH
    REMOTE_PATH = data[2].split()[2]
    global LAUNCH_FROM
    LAUNCH_FROM = data[3].split()[2]
    global REPLACE_WHICH
    REPLACE_WHICH = data[4].split()[2]
    global BUCKET
    BUCKET = data[5].split()[2]
    global BUCKET_STR
    BUCKET_STR = '--bucket ' + BUCKET
    global PREFIX_SUFFIX
    PREFIX_SUFFIX = data[6].split()[2]
    global VIRT_TYPE
    VIRT_TYPE = data[7].split()[2]
    global VIRT_TYPE_STR
    VIRT_TYPE_STR = '--virtualization-type ' + VIRT_TYPE

    f2 = open(CLOUD + '/' + CLOUD + '-params', 'r')
    data2 = f2.readlines()
    global BASE_IP
    BASE_IP = data2[0].split()[2]
    global REGION
    REGION = data2[1].split()[2]
    global REGION_STR
    REGION_STR = '--region ' + REGION
    global SECURITY_GROUP
    SECURITY_GROUP = data2[2].split()[2]
    global SECURITY_GROUP_STR
    SECURITY_GROUP_STR = '--group ' + SECURITY_GROUP
    global BASE_IMAGE
    BASE_IMAGE = data2[3].split()[2]

