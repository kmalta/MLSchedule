import sys
import os

def get_percentage_of_file(path, new_path, amount, sample_bool):
    os.system("wc -l " + path + " | awk '{print $1}' > /home/ubuntu/lines")
    f = open(path, 'r')
    f2 = open(new_path, 'w')
    f3 = open('/home/ubuntu/lines', 'r')
    total_lines = int(f3.readlines()[0].strip())
    f3.close()

    line_count = int(amount*total_lines)
    count = 0
    for line in f:
        try:
            arr=line.split()
            l = map(lambda x: int(x.split(':')[0]), arr[1:])
            if int(arr[0]) not in [0,1]:
                continue
            if all(l[i] <= l[i+1] for i in xrange(len(l)-1)):
                1
            else:
                continue
        except:
            print arr[0]
            print l
            continue
        count += 1
        if sample_bool == True:
            if count == amount:
                break
        if count % 10000 == 0:
            print 'Processing Line:', str(count)
        if count > line_count and sample_bool == False:
            break
        f2.write(line)
    f.close()
    f2.close()
    print "Number of lines:", str(count)


def main():
    clp = sys.argv
    print clp
    path = clp[1]
    new_path = path + '_exp'
    amount = float(clp[2])
    try:
        sample_bool = bool(clp[3])
        if sample_bool == True:
            amount = int(amount)
    except:
        print "Did not get bool"
    get_percentage_of_file(path, new_path, amount, sample_bool)

if __name__ == "__main__":
    main()