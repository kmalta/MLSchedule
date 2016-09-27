import sys


def covert_file(path, num_feats):
    fp = open(path, 'r')
    fp2 = open(path + '.txt', 'w')
    count = 0
    for line in fp:
        count += 1
        if count % 100000 == 0:
            print 'Processing Line:', str(count)
        array = ['0' for j in range(num_feats)]
        cols = line.split()
        for col in cols[1:]:
            feat = col.split(':')
            try:
                array[int(feat[0]) - 1] = feat[1]
            except:
                print '\nERROR\n\n'
                print line
                print feat
                print count
                print '\n\n'
        array.insert(0, cols[0])
        fp2.write(','.join(array) + '\n')


def convert_classes(path):
    fp = open(path, 'r')
    fp2 = open('temp', 'w')
    count = 0
    for line in fp:
        count += 1
        if count % 100000 == 0:
            print 'Processing Line:', str(count)
        data = line.split()
        if data[0] == '0':
            data[0] = '-1'
        fp2.write(' '.join(data) + '\n')
    fp.close()
    fp2.close()
    #os.system('mv temp ' + path)


def change_classes_to_0_idx(path, new_path):
    f = open(path, 'r')
    f2 = open(new_path, 'w')

    count = 0
    for line in f:
        count += 1
        if count % 10000 == 0:
            print 'Processing Line:', str(count)
        data_arr = line.split()
        if data_arr[0] in ['-1', '+1']:
            data_arr[0] = '0' if data_arr[0] == '-1' else '1' #str(int(data_arr[0]) - 1)
        else:
            data_arr[0] = str(int(data_arr[0]) - 1)
        for i, feat in enumerate(data_arr[1:]):
            feature_vec = feat.split(':')
            data_arr[i+1] = feature_vec[0]+ ':' + str(int(float(feature_vec[1])))
        f2.write(' '.join(data_arr) + '\n')
    f.close()
    f2.close()


def main():
    #Make sure to put the data path then the number of features of the data set
    clp = sys.argv
    path = clp[1]
    try:
        new_path = clp[2]
    except:
        1
    #num_features = int(clp[2])
    #num_features = 54
    #covert_file(path, num_features)
    #convert_classes(path)
    change_classes_to_0_idx(path, new_path)


if __name__ == "__main__":
    main()