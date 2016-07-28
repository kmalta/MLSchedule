#GLOBALS
CLOUD = ''
OPTIONS = ''
REMOTE_PATH = ''
LAUNCH_FROM = ''
REPLACE_WHICH = ''
PEM_PATH = ''
S3CMD_CFG_PATH = ''
REGION = ''
BASE_IP = ''
KEY = ''
KEY_STR = ''
BASE_IP = ''
REGION = ''
REGION_STR = ''
SECURITY_GROUP = ''
BASE_IMAGE = ''
BUCKET = ''
BUCKET_STR = ''
PREFIX_SUFFIX = ''
VIRT_TYPE = ''
VIRT_TYPE_STR = ''


def set_globals():
    f = open('./script-cfg', 'r')
    data = f.readlines()
    CLOUD = data[0].split()[2]
    KEY = CLOUD + '-key'
    KEY_STR = '--key ' + KEY
    PEM_PATH = CLOUD + '/' + KEY + '.pem'
    KEY = CLOUD + '-key'
    OPTIONS = ' '.join(data[1].split()[2:]) + ' ' + PEM_PATH
    REMOTE_PATH = data[2].split()[2]
    LAUNCH_FROM = data[3].split()[2]
    REPLACE_WHICH = data[4].split()[2]
    BUCKET = data[5].split()[2]
    BUCKET_STR = '--bucket ' + BUCKET
    PREFIX_SUFFIX = data[6].split()[2]
    VIRT_TYPE = data[7].split()[2]
    VIRT_TYPE_STR = '--virtualization-type ' + VIRT_TYPE

    f2 = open(CLOUD + '/' + CLOUD + '-params', 'r')
    data2 = f2.readlines()
    BASE_IP = data2[0].split()[2]
    REGION = data2[1].split()[2]
    REGION_STR = '--region ' + REGION
    SECURITY_GROUP = data2[2].split()[2]
    SECURITY_GROUP_STR = '--group ' + SECURITY_GROUP
    BASE_IMAGE = data2[3].split()[2]

