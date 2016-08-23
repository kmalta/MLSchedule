import urllib
import sys

def remove_html_tags(string):
    return string.split('>')[1].split('<')[0]


def parse_bid(string):
    splat = string.split('$')[1].split('(')
    if 'none' not in splat[0]:
        return [float(splat[0].strip()), float(splat[1].split(')')[0])]
    else:
        return ['none', 'none']

def parse_duration(string):
    splat = string.split()
    if 'none' not in splat[2]:
        return [float(splat[0]), float(splat[2].split('(')[1].split(')')[0])]
    else:
        return ['none', 'none']

def get_bid(inst_type, region):
    url = 'http://128.111.84.183/by-inst.html'
    f = urllib.urlopen(url)
    data = f.readlines()
    first_idx = -1
    for i, line in enumerate(data):
        if '<h2>' in line:
            if remove_html_tags(line) == inst_type:
                first_idx = i

    value_to_parse = ''
    time_to_parse = ''
    for i, line in enumerate(data[first_idx:]):
        if '<td>' in line:
            if remove_html_tags(line) == region:
                value_to_parse = remove_html_tags(data[first_idx:][i+2])
                time_to_parse = remove_html_tags(data[first_idx:][i+3])
                break

    ret_dict = {}
    ret_dict['inst_type'] = inst_type
    ret_dict['region'] = region
    ret_dict['bid'] = parse_bid(value_to_parse)
    ret_dict['duration'] = parse_duration(time_to_parse)
    print repr(ret_dict)
    return ret_dict



def main():
    clp = sys.argv
    inst_type = clp[1]
    region = clp[2]
    get_bid(inst_type, region)

if __name__ == "__main__":
    main()

