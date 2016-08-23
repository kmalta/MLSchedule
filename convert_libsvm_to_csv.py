import sys


def covert_file(path, num_feats):
    fp = open(path, 'r')
    fp2 = open(path + '.csv', 'w')
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
        array.append(cols[0])
        fp2.write(','.join(array) + '\n')


def main():
    #Make sure to put the data path then the number of features of the data set
    clp = sys.argv
    path = clp[1]
    num_features = int(clp[2])

    covert_file(path, num_features)


if __name__ == "__main__":
    main()