import urllib
import sys
import ast

region = 'us-west-1'


def get_cost(num_machines, inst_type):
    ret_dict = get_bid(inst_type, region)
    return ret_dict['bid']*num_machines


def get_bid(inst_type, region, hours):
    url = 'http://128.111.84.183/' + region + '-' + inst_type + '.html'
    f = urllib.urlopen(url)
    data = f.readlines()
    script_index = -1
    for i, line in enumerate(data):
        if '<script>' in line:
            script_index = i
            break

    time_line = data[script_index + 2]
    cost_line = data[script_index + 3]


    times = ast.literal_eval('[' + time_line.split('[')[1].split(']')[0] + ']')
    costs = ast.literal_eval('[' + cost_line.split('[')[1].split(']')[0] + ']')

    hours_cliff_index = -1
    for i, elem in enumerate(times):
        if int(elem) >= hours:
            hours_cliff_index = i
            break

    time = float(times[i])
    price = float(costs[i])

    ret_dict = {}
    ret_dict['inst_type'] = inst_type
    ret_dict['region'] = region
    ret_dict['bid'] = price
    ret_dict['duration'] = time
    return ret_dict














def main():
    inst_types = ['c4.xlarge', 'm4.xlarge', 'r4.2xlarge']

    for inst_type in inst_types:
        ret = get_bid(inst_type, region, 1)
        print ret['inst_type']
        print ret['bid']
        print ret['duration']
        print "Number of machines for $1:", str(1.00/ret['bid'])
        print 
        print


    # clp = sys.argv
    # inst_type = clp[1]
    # region = clp[2]
    # get_bid(inst_type, region)

if __name__ == "__main__":
    main()

