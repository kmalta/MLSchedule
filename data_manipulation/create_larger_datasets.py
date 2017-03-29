import os
import sys


coord_strs = ['1_0',
'2_0',
'3_0',
'4_-1',
'5_-2',
'6_-3',
'7_-4',
'8_-5',]

size = int(sys.argv[1])

for coord_str in coord_strs:
    file_name = 'synth_data_' + coord_str + '_0'
    os.system('s3cmd -c aristotle/aristotle-s3cfg get s3://synth-datasets-' + str(size/2) + 'gb/' + file_name + ' fake_datasets/' + file_name)
    os.system('cat fake_datasets/' + file_name + ' fake_datasets/' + file_name + ' > fake_datasets/' + file_name + '_tmp')
    os.system('s3cmd -c aristotle/aristotle-s3cfg put fake_datasets/' + file_name + '_tmp' + ' s3://synth-datasets-' + str(size) + 'gb/' + file_name)
    os.system('rm fake_datasets/' + file_name)
    os.system('rm fake_datasets/' + file_name + '_tmp')


for coord_str in coord_strs:
    file_name = 'synth_data_' + coord_str + '_0'
    os.system('s3cmd -c aristotle/aristotle-s3cfg get s3://synth-datasets-' + str(size) + 'gb/' + file_name + ' fake_datasets/' + file_name)
    os.system('cat fake_datasets/' + file_name + ' fake_datasets/' + file_name + ' > fake_datasets/' + file_name + '_tmp')
    os.system('s3cmd -c aristotle/aristotle-s3cfg put fake_datasets/' + file_name + '_tmp' + ' s3://synth-datasets-' + str(size*2) + 'gb/' + file_name)
    os.system('rm fake_datasets/' + file_name)
    os.system('rm fake_datasets/' + file_name + '_tmp')
