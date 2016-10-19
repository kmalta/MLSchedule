from scipy.stats import truncnorm
from random import randint, random, sample
import math


#need to specify samples, classes, and features
dataset_to_replicate = 'generated_higgs_0' #'experiment_data/higgs/higgs_0'
num_samples = 11000000
num_features = 28
num_classes = 2
total_bytes = 7937260828
feature_types = 'doubles'


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
  right_of_decimal = [str(randint(0,9)) for i in range(17)]
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



tot, mean, var, max_, min_ = compute_stats()
#tot, mean, var, max_, min_ = 283685620, 25.7896018182, 0.184461877775, 26, 24

print str(tot), str(mean), str(var), str(max_), str(min_)

#a,b = (min_ - mean)/math.sqrt(var), (max_ - mean)/math.sqrt(var)

#trunc_norm_generation(mean, math.sqrt(var), a, b, 'generated_higgs_0')
#low_info_data_set_gen(mean, 'generated_low_info_higgs_0')
