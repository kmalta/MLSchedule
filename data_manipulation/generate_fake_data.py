from scipy.stats import truncnorm
from random import randint, random, sample
import math
import sys
import time
import ast
import os


def compute_stats():
  f = open(dataset_to_replicate, 'r')
  sum_total = 0
  min_num_feats = num_features + 1
  max_num_feats = 0
  count = 0
  for line in f:
    count += 1
    if count % 100000 == 0:
      print "Processing line", str(count), "for the mean."
    num_feats = len(line.split()) - 1
    sum_total += num_feats
    if num_feats < min_num_feats:
      min_num_feats = num_feats
    if num_feats > max_num_feats:
      max_num_feats = num_feats

  sample_mean = float(sum_total)/num_samples

  sum_total_for_variance = 0

  f.close()
  f = open(dataset_to_replicate, 'r')
  count = 0
  for line in f:
    count += 1
    if count % 100000 == 0:
      print "Processing line", str(count), "for the variance."
    num_feats = len(line.split()) - 1
    sum_total_for_variance += (num_feats-sample_mean)**2

  f.close()

  sample_variance = sum_total_for_variance/num_samples

  return sum_total, sample_mean, sample_variance, max_num_feats, min_num_feats


def trunc_norm_generation(mean, std, a, b, file_path):
  f = open(file_path, 'w')
  ave_feats = int(round(mean))
  chars_per_feat = compute_ave_chars_per_feat(ave_feats)
  count = 0
  feature_samples = truncnorm.rvs(a, b, size=num_samples)
  for i in range(num_samples):
    count += 1
    if count % 100000 == 0:
      print "Processing line", str(count), "for the generated dataset."
    label = randint(0, num_classes - 1)
    feature_sample = int(round(feature_samples[i]*std+mean))
    feature_labels = sample(range(1, num_features + 1), feature_sample)
    feature_labels.sort()
    features = [str(feature_labels[i]) + ':' + get_random_data_point() for i in range(feature_sample)]
    line_to_write = str(label) + ' ' + ' '.join(features) + '\n'
    f.write(line_to_write)


def get_random_data_point():
  sign = '-' if randint(0, 1) == 0 else ''
  sign2 = '+00' if randint(0, 1) == 0 else '-01'
  left_of_decimal = str(randint(1,9))
  right_of_decimal = [str(randint(0,9)) for i in range(5)]
  right_of_decimal = ''.join(right_of_decimal)
  right_of_decimal += str(randint(1,9))
  num = sign + left_of_decimal + '.' + right_of_decimal + 'e' + sign2
  return num


def get_random_data_point_fixed_feat(num_chars):
  sign = '-' if randint(0, 1) == 0 else ''
  sign2 = '+00' if randint(0, 1) == 0 else '-01'
  left_of_decimal = str(randint(1,9))
  right_of_decimal = [str(randint(0,9)) for i in range(num_chars - 1)]
  right_of_decimal = ''.join(right_of_decimal)
  right_of_decimal += str(randint(1,9))
  num = sign + left_of_decimal + '.' + right_of_decimal + 'e' + sign2
  return num

def compute_ave_chars_per_feat(num_feats):
  bytes_per_line = total_bytes / num_samples
  feat_len = len(''.join([str(i) for i in range(1, num_feats+1)]))
  #class  1 space  1 left 1 colon 1 decimal 4 sci_not 
  chars_per_feat = (bytes_per_line - 1 - 8*num_feats - feat_len)/num_feats
  print chars_per_feat
  return chars_per_feat

def low_info_data_set_gen(mean, file_path):
  ave_feats = int(round(mean))
  chars_per_feat = compute_ave_chars_per_feat(ave_feats)
  f = open(file_path, 'w')
  count = 0
  for i in range(num_samples):
    count += 1
    if count % 100000 == 0:
      print "Processing line", str(count), "for the generated dataset."
    label = randint(0, num_classes - 1)
    feature_labels = sample(range(1, num_features + 1), ave_feats)
    feature_labels.sort()
    features = [str(feature_labels[i]) + ':' + get_random_data_point_fixed_feat(chars_per_feat) for i in range(ave_feats)]
    line_to_write = str(label) + ' ' + ' '.join(features) + '\n'
    f.write(line_to_write)


def datasets_generation_for_exps(sample_factor, samples, features, sparsity, file_name, send_name, send_flag, byte_size):
  f = open('fake_datasets/' + file_name, 'w')

  samples = int(sample_factor*samples)
  print "Samples:", str(int(samples))


  ave_feats = int(sparsity*features)
  chars_per_feat = 8


  if features*sparsity > 10:
    checkpoint = 1e2
  else:
    checkpoint = 1e3

  count = 0
  rng = range(1, features + 1)
  if features == 1:
    rng = [rng]
  prev_time = time.time()
  if samples != features:
    for i in range(samples):
      count += 1
      if count % checkpoint == 0:
        new_time = time.time()
        diff = new_time - prev_time
        print "Processing line", str(count), "for the generated dataset. Time taken:", str(diff)
        prev_time = new_time
      label = randint(0, 1)
      feature_labels = sample(rng, ave_feats)
      feature_labels.sort()
      features = [str(feature_labels[i]) + ':' + get_random_data_point_fixed_feat(chars_per_feat) for i in range(ave_feats)]
      line_to_write = str(label) + ' ' + ' '.join(features) + '\n'
      f.write(line_to_write)
  else:
      for i in range(samples):
        count += 1
        if count % checkpoint == 0:
          new_time = time.time()
          diff = new_time - prev_time
          print "Processing line", str(count), "for the generated dataset. Time taken:", str(diff)
          prev_time = new_time
        label = randint(0, 1)
        feature_labels = rng
        features = [str(feature_labels[i]) + ':' + get_random_data_point_fixed_feat(chars_per_feat) for i in range(ave_feats)]
        line_to_write = str(label) + ' ' + ' '.join(features) + '\n'
        f.write(line_to_write)

  sample_str = ''
  if sample_factor == 10:
    sample_str = '1m'
  elif sample_factor == 1:
    sample_str = '100k'
  elif sample_factor == .1:
    sample_str = '10k'
  elif sample_factor == 2:
    sample_str = '200k'
  elif sample_factor == 4:
    sample_str = '400k'
  elif sample_factor == .5:
    sample_str = '50k'
  elif sample_factor == .25:
    sample_str = '25k'
  elif sample_factor == .02:
    sample_str = '2k'
  else:
    print sample_factor
    #sys.exit('ERROR EXITING')

  if byte_size != '-1':
    sample_str = str(byte_size) + 'gb'



  if send_flag == True:
    os.system('s3cmd -c aristotle/aristotle-s3cfg put fake_datasets/' + file_name + ' s3://synth-datasets-' + sample_str +'/' + send_name + '_0')
    os.system('rm fake_datasets/' + file_name)



def time_str():
    return time.strftime("-%Y-%m-%d-%H-%M-%S", time.gmtime())

clp = sys.argv
samples = 1e5#ast.literal_eval('1e' + clp[1])
features = ast.literal_eval('1e' + clp[1])
sparsity = ast.literal_eval('1e' + clp[2])
send_name = 'synth_data_' + clp[1] + '_' + clp[2]
file_name = 'synth_data_' + clp[1] + '_' + clp[2] + time_str()
try:
  sample_factor = ast.literal_eval(clp[3])
except:
  sample_factor = 1

size_required = 0
if clp[4] == 'size':
  datasets_generation_for_exps(sample_factor, int(samples), int(features), sparsity, file_name, send_name, False, clp[5])
  size_in_bytes = os.path.getsize('fake_datasets/' + file_name)
  os.system('rm fake_datasets/' + file_name)
  size_required = ast.literal_eval(clp[5])
  samples = size_required*(1e9)/(size_in_bytes/float(samples))

print
print
print "CREATING ", clp[1], clp[2]
print
print

datasets_generation_for_exps(sample_factor, int(samples), int(features), sparsity, file_name, send_name, True, clp[5])




# tot, mean, var, max_, min_ = compute_stats()
# #tot, mean, var, max_, min_ = 283685620, 25.7896018182, 0.184461877775, 26, 24

#'20_gb_10_features_synthetic'
# print str(tot), str(mean), str(var), str(max_), str(min_)
# num_samples = 50000000
# num_features = 10
# num_classes = 2
# total_bytes = 21474836480
# feature_types = 'doubles'
# mean = 8
# var = 1
# min_ = 4
# max_ = 10
# a,b = (min_ - mean)/math.sqrt(var), (max_ - mean)/math.sqrt(var)
# trunc_norm_generation(mean, math.sqrt(var), a, b, '20_gb_10_features_synthetic')





# '10_gb_10000_feature_ave_synthetic'
# num_samples = 100000
# num_features = 100000
# num_classes = 2
# total_bytes = 10737418240/4
# feature_types = 'doubles'
# mean = 10000
# var = 10000
# min_ = 5000
# max_ = 15000
# a,b = (min_ - mean)/math.sqrt(var), (max_ - mean)/math.sqrt(var)
# trunc_norm_generation(mean, math.sqrt(var), a, b, '10_gb_10000_feature_ave_synthetic')


# '30_gb_100000_feature_100_ave_synthetic'
#bytes = 31414001500
#lines = 10000000


# num_samples = 1000000
# num_features = 100000
# num_classes = 2
# total_bytes = 10737418240/2
# feature_types = 'doubles'
# mean = 10000
# var = 10000
# min_ = 67
# max_ = 173
# a,b = (min_ - mean)/math.sqrt(var), (max_ - mean)/math.sqrt(var)
# trunc_norm_generation(mean, math.sqrt(var), a, b, '10_gb_10000_feature_ave_synthetic')