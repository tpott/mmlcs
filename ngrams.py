# ngrams.py
# Trevor Pottinger
# Tue May 12 22:56:34 PDT 2015

from __future__ import print_function

import argparse
import glob
import json
import multiprocessing
import os
import sys
import time

DEBUG = False
ENABLE_MULTICORE = True
NUM_CORES = multiprocessing.cpu_count()
NGRAMS = 3

def ngrams(data, n):
  "Expects a bytestring and returns a histogram of ngrams"
  assert n >= 0, 'n must be greater than zero: %d' % n
  assert n < len(data), 'n must be less than len(data): %d > %d' % (n, len(data))
  hist = {}
  for i in xrange(len(data) - n + 1):
    # I feel like this one line is why I use python..
    gram = data[i : i + n]
    if gram in hist:
      hist[gram] += 1
    else:
      hist[gram] = 1
  return hist

def substrings(data, n, hist):
  "Assumes hist is a bunch of ngrams"
  subs = set()
  i = 0
  while i < len(data) - n + 1:
    if data[i : i + n] in hist:
      # select longest substring in hist
      end = i + 1
      for j in xrange(i + 1, len(data) - n + 1):
        if data[j : j + n] in hist:
          end += 1
        else:
          break
      # TODO use a constant instead of 4
      if end - i > 4:
        subs.add(data[i : end])
        i = end
    # if only python had real for loops and this bug wouldn't have happened..
    i += 1
  return subs

def sortedHist(hist, minT=0):
  "Actually returns a sorted list of (key, value) tuples"
  tuples = hist.items()
  if minT > 0:
    tuples = filter(lambda kvtuple: kvtuple[1] > minT, tuples)
  def __cmp(x, y):
    if x[1] > y[1]:
      return 1
    elif x[1] < y[1]:
      return -1
    else:
      return 0
  tuples.sort(__cmp, reverse=True)
  return tuples

def topKHist(tuples, k):
  "Expects tuples to be sorted (key, value) tuples, lowest first"
  ret = {}
  for i in range(k):
    ret[tuples[-i][0]] = tuples[-i][1]
  return ret

def prettybytes(s):
  return ''.join( ("%02x" % ord(c) for c in s) )

def prettyhist(hist):
  "Expects a histogram where the keys are bytestrings"
  ret = {}
  for gram in hist:
    hexgram = prettybytes(gram)
    ret[hexgram] = hist[gram]
  return ret

def percentiles(ns, k):
  "Expects a list of integers and the number of percentiles"
  assert len(ns) > k, 'There must be more integers than k: %d <= %d' % (len(ns), k)
  # TODO expects ns to be sorted
  ret = []
  # TODO len(ns) / k is not the correct step size
  # I'd expect len(ret) == k + 1, but it's not
  for i in range(0, len(ns), len(ns) / k):
    ret.append(ns[i])
  ret.append(ns[-1])
  return ret

def simpleNGramFun(filenames):
  lens = []
  count_distinct_ngrams = []
  common_ngrams = {}
  # TODO partition the problem size and use multiprocessing
  for filename in filenames:
    blob = open(filename).read()
    # SLOW +1
    hist = ngrams(blob, NGRAMS)
    lens.append(len(blob))
    count_distinct_ngrams.append(len(hist))
    # SLOW +5
    for k in hist:
      if k in common_ngrams:
        common_ngrams[k] += 1
      else:
        common_ngrams[k] = 1
    if DEBUG:
      print("%s %d %d" % (filename, len(blob), len(hist)))
  return (lens, count_distinct_ngrams, common_ngrams)

def multiNGramFun(filenames):
  filename_partitions = []
  partition_size = len(filenames) / NUM_CORES
  for i in range(NUM_CORES):
    filename_partitions.append(filenames[i*partition_size:(i+1)*partition_size])
  pool = multiprocessing.Pool(NUM_CORES)
  result_partitions = pool.map(simpleNGramFun, filename_partitions)
  # not sure why these next two lines are necessary
  pool.close()
  pool.join()
  # now to join all the results
  lens = []
  count_distinct_ngrams = []
  common_ngrams = {}
  for i in range(NUM_CORES):
    lens.extend(result_partitions[i][0])
    count_distinct_ngrams.extend(result_partitions[i][1])
    partial_common_ngrams = result_partitions[i][2]
    # SLOW +5
    for k in partial_common_ngrams:
      if k in common_ngrams:
        common_ngrams[k] += partial_common_ngrams[k]
      else:
        common_ngrams[k] = partial_common_ngrams[k]
  return (lens, count_distinct_ngrams, common_ngrams)

def simpleSubstringsFun(args):
  """Approximates longest common substring by extracting substrings that are
  composed of several ngrams that occured in many distinct file contents"""
  filenames = args[0]
  ngram_set = args[1]
  common_substrings = {}
  for filename in filenames:
    blob = open(filename).read()
    subs = substrings(blob, NGRAMS, ngram_set)
    for sub in subs:
      if sub in common_substrings:
        common_substrings[sub] += 1
      else:
        common_substrings[sub] = 1
  return common_substrings

def multiSubstringsFun(args):
  filenames = args[0]
  ngram_set = args[1]
  filename_partitions = []
  partition_size = len(filenames) / NUM_CORES
  for i in range(NUM_CORES):
    filename_partitions.append((filenames[i*partition_size:(i+1)*partition_size], ngram_set))
  pool = multiprocessing.Pool(NUM_CORES)
  result_partitions = pool.map(simpleSubstringsFun, filename_partitions)
  # not sure why these next two lines are necessary
  pool.close()
  pool.join()
  # now join the results
  common_substrings = {}
  for i in range(NUM_CORES):
    partial_common_substrings = result_partitions[i]
    for k in partial_common_substrings:
      if k in common_substrings:
        common_substrings[k] += partial_common_substrings[k]
      else:
        common_substrings[k] = partial_common_substrings[k]
  return common_substrings

def main(path_regex, outfile, outformat):
  start = time.time()
  filenames = glob.glob(path_regex)
  if not ENABLE_MULTICORE:
    (lens, count_distinct_ngrams, common_ngrams) = simpleNGramFun(filenames)
  else:
    (lens, count_distinct_ngrams, common_ngrams) = multiNGramFun(filenames)
  now = time.time()
  print("[+] Reading %d files complete; time elapsed: %1.3f" % (len(lens), now - start))
  start = now
  # not slow because their len is the number of samples
  lens.sort(reverse=True)
  count_distinct_ngrams.sort(reverse=True)
  if not ENABLE_MULTICORE:
    # SLOW +1 because common_ngrams gets huge
    sorted_common_ngrams = sortedHist(common_ngrams)
  else:
    # TODO multi core sort
    sorted_common_ngrams = sortedHist(common_ngrams)
  # these next three are really just for pretty printing
  # begin can this be removed?
  sorted_duplicated_common_ngrams = filter(lambda kvtuple: kvtuple[1] > 1, sorted_common_ngrams)
  pretty_common_ngrams = map(lambda kvtuple: (prettybytes(kvtuple[0]), kvtuple[1]), sorted_common_ngrams)
  pretty_duplicated_ngrams = map(lambda kvtuple: (prettybytes(kvtuple[0]), kvtuple[1]), sorted_duplicated_common_ngrams)
  now = time.time()
  print("[+] Sorting distinct ngrams complete; time elapsed: %1.3f" % (now - start))
  start = now
  # end can this be removed?
  # TODO how do we pick 30?
  top_common_ngram_set = set(map(lambda kvtuple: kvtuple[0], filter(lambda kvtuple: kvtuple[1] > 30, sorted_common_ngrams)))
  if not ENABLE_MULTICORE:
    # SLOW +1
    common_substrings = simpleSubstringsFun((filenames, top_common_ngram_set))
  else:
    common_substrings = multiSubstringsFun((filenames, top_common_ngram_set))
  now = time.time()
  print("[+] Extracting %d substrings complete; time elapsed: %1.3f" % (len(common_substrings), now - start))
  start = now
  if not ENABLE_MULTICORE:
    # This shouldn't be too slow since its sample size is much smaller than above
    # Note that this returns a list of (substring, count) tuples
    sorted_common_substrings = sortedHist(common_substrings, 1)
  else:
    sorted_common_substrings = sortedHist(common_substrings, 1)
  # TODO don't take the first 40 bytes...
  pretty_common_substrings = map(lambda kvtuple: (prettybytes(kvtuple[0][:40]), kvtuple[1]), sorted_common_substrings)
  pretty_common_substrings_raw = map(lambda kvtuple: (prettybytes(kvtuple[0]), kvtuple[1]), sorted_common_substrings)
  substring_lens = map(lambda kvtuple: len(kvtuple[0]), sorted_common_substrings)
  substring_lens.sort(reverse=True)
  now = time.time()
  print("[+] Sorting distinct substrings complete; time elapsed: %1.3f" % (now - start))
  start = now
  if outfile is not None:
    assert outformat is not None, 'outformat should never be None'
    f = open(outfile, 'w')
    if outformat == 'json':
      print("[+] Writing output to %s" % (outfile))
      f.write("%s\n" % pretty_common_substrings_raw)
    elif outformat == 'tsv':
      print('TSV output format not yet implemeneted')
    else:
      print("Unknown output format %s" % outformat)
    f.close()
  print("\tLength percentiles: %s" % (json.dumps(percentiles(lens, 15))))
  print("\tDistinct ngram percentiles: %s" % (json.dumps(percentiles(count_distinct_ngrams, 15))))
  #print("\tTop K ngrams: %s" % (json.dumps(prettyhist(topKHist(sorted_common_ngrams, 80)))))
  print("\tShared ngram percentiles: %s" % (json.dumps(percentiles(pretty_common_ngrams, 15))))
  print("\tShared ngram (>1) percentiles: %s" % (json.dumps(percentiles(pretty_duplicated_ngrams, 15))))
  print("\tNum total distinct ngrams: %d" % (len(common_ngrams)))
  print("\tNum total distinct ngrams (>1 occurance): %d" % (len(sorted_duplicated_common_ngrams)))
  #print("\tSample of substrings: %s" % (json.dumps(prettyhist(common_substrings).items()[:30])))
  print("\tShared substring percentiles: %s" % (json.dumps(percentiles(pretty_common_substrings, 15))))
  print("\tSubstring length percentiles: %s" % (json.dumps(percentiles(substring_lens, 15))))
  print("\tNum total distinct substrings: %d" % (len(substring_lens)))

def validateInput(args):
  # input directory
  if args.input_dir is None:
    raise Exception('input_dir is needed')
  elif not os.path.isdir(args.input_dir):
    raise Exception("%s is not a directory" % args.input_dir)
  else:
    input_dir = "%s/*" % args.input_dir
  # output file
  if args.output is not None and not os.path.exists(args.output):
    output = args.output
  elif args.output is not None and os.path.isdir(args.output):
    raise Exception("%s is a directory, can't write to it" % args.output)
  elif args.output is None:
    output = None
  else:
    # TODO verify output file is writable
    print("[-] WARNING: Don't know what to do with %s, assuming None" % args.output)
    output = None
  # output format
  if args.format is None:
    output_format = 'json'
  elif args.format.lower() == 'json':
    output_format = 'json'
  elif args.format.lower() == 'tsv':
    output_format = 'tsv'
  else:
    print("[-] WARNING: Unknown output format %s, assuming json" % args.format)
    output_format = 'json'
  return (input_dir, output, output_format)

if __name__ == '__main__':
  # TODO argparse
  parser = argparse.ArgumentParser(
    description='Approximates longest common substring'
  )
  parser.add_argument(
    'input_dir',
    help='Where the data files are stored that should be read'
  )
  parser.add_argument('-o', '--output', help='Where to store the results')
  parser.add_argument(
    '-f',
    '--format',
    help='How the output should be formated. TSV or JSON'
  )
  (input_dir_regex, output, outformat) = validateInput(parser.parse_args())
  main(input_dir_regex, output, outformat)
