import sys
import os

def get_percentage_of_file(path, new_path, percentage):
    os.system("wc -l " + path + " | awk '{print $1}' > /home/ubuntu/lines")
    f = open(path, 'r')
    f2 = open(new_path, 'w')
    f3 = open('/home/ubuntu/lines', 'r')
    total_lines = int(f3.readlines()[0].strip())
    f3.close()

    line_count = int(percentage*total_lines)
    count = 0
    for line in f:
        count += 1
        if count % 10000 == 0:
            print 'Processing Line:', str(count)
        if count > line_count:
            break
        f2.write(line)
    f.close()
    f2.close()


def main():
    clp = sys.argv
    print clp
    path = clp[1]
    new_path = path + '_exp'
    percentage = float(clp[2])
    get_percentage_of_file(path, new_path, percentage)

if __name__ == "__main__":
    main()