# cooccurrences.py
# Trevor Pottinger
# Wed Jun 10 21:56:20 PDT 2015
#
# This is arguably more expensive than substring extraction. There seems to be
# a lot more academic articles on this operation.

from __future__ import print_function

# stdlib imports
import argparse
import json
import multiprocessing
import random
import sys
import time

# TODO mergeSort isn't really mergeSort
from sorting import (mergeSort, multiMergeSort)

# TODO use it from mmlcs
def __hist_cmp(x, y):
  if x[1] > y[1]:
    return 1
  elif x[1] < y[1]:
    return -1
  else:
    return 0

# TODO use it from mmlcs
def sortedHist(hist, minT=0):
  "Actually returns a sorted list of (key, value) tuples"
  tuples = hist.items()
  if minT > 0:
    tuples = filter(lambda kvtuple: kvtuple[1] > minT, tuples)
  # True implies reverse=True, aka DESCENDING
  return mergeSort(tuples, __hist_cmp, True)

def readFile(input_db):
  num_lines_read = 0
  file_to_substr = {}
  substr_to_file = {}
  with open(input_db) as f:
    for l in f:
      # TODO how can wer verify the input format?
      # TODO not using file_offset
      (file_hash, substr_hash, file_offset) = l.strip().split("\t")
      if file_hash in file_to_substr:
        file_to_substr[file_hash].add(substr_hash)
      else:
        file_to_substr[file_hash] = set([substr_hash])
      if substr_hash in substr_to_file:
        substr_to_file[substr_hash].add(file_hash)
      else:
        substr_to_file[substr_hash] = set([file_hash])
      num_lines_read += 1
    return (num_lines_read, file_to_substr, substr_to_file)

def bruteForceCooccurr(file_to_substr, substr_to_file):
  cooccurrences = {}
  for file_hash in file_to_substr:
    substr_list = list(file_to_substr[file_hash])
    substr_list.sort()
    # this is the really expensive loop
    for i in range(len(substr_list)):
      for j in range(i+1, len(substr_list)):
        # sorting co-occurrence since order doesn't matter
        if substr_list[i] < substr_list[j]:
          pair = (substr_list[i], substr_list[j])
        else:
          pair = (substr_list[j], substr_list[i])
        if pair in cooccurrences:
          cooccurrences[pair].add(file_hash)
        else:
          cooccurrences[pair] = set([file_hash])
  return cooccurrences

def topKCooccurr(file_to_substr, substr_to_file, topKSubstrs):
  topKSet = set(map(lambda item: item[0], topKSubstrs))
  cooccurrences = {}
  for file_hash in file_to_substr:
    substr_list = list(file_to_substr[file_hash])
    substr_list.sort()
    substr_list = filter(lambda substr: substr in topKSet, substr_list)
    # this is the really expensive loop
    for i in range(len(substr_list)):
      for j in range(i+1, len(substr_list)):
        # sorting co-occurrence since order doesn't matter
        if substr_list[i] < substr_list[j]:
          pair = (substr_list[i], substr_list[j])
        else:
          pair = (substr_list[j], substr_list[i])
        if pair in cooccurrences:
          cooccurrences[pair].add(file_hash)
        else:
          cooccurrences[pair] = set([file_hash])
  return cooccurrences

def sampledCooccurr(file_to_substr, substr_to_file, sampling_rate):
  cooccurrences = {}
  for file_hash in file_to_substr:
    substr_list = list(file_to_substr[file_hash])
    substr_list.sort()
    n_samples = int(float(len(substr_list)) / sampling_rate)
    for k in range(n_samples):
      i, j = None, None
      while i is None or i == j:
        i = random.randint(0, len(substr_list) - 1)
        j = random.randint(0, len(substr_list) - 1)
      # sorting co-occurrence since order doesn't matter
      if substr_list[i] < substr_list[j]:
        pair = (substr_list[i], substr_list[j])
      else:
        pair = (substr_list[j], substr_list[i])
      if pair in cooccurrences:
        cooccurrences[pair].add(file_hash)
      else:
        cooccurrences[pair] = set([file_hash])
  return cooccurrences

def genericCooccurr(file_to_substr, substr_to_file, sampling_rate, top_k, topKSubstrs):
  if sampling_rate != 0:
    return sampledCooccurr(file_to_substr, substr_to_file, sampling_rate)
  elif top_k != 0 and topKSubstrs is not None:
    return topKCooccurr(file_to_substr, substr_to_file, topKSubstrs)
  else:
    return bruteForceCooccurr(file_to_substr, substr_to_file)

def main(input_db, tabular, sampling_rate, top_k):
  start = time.time()
  (num_lines_read, file_to_substr, substr_to_file) = readFile(input_db)
  now = time.time()
  print("[+] Reading %d lines, %d file hashes, and %d substr hashes complete; time elapsed: %1.3f" % (
    num_lines_read,
    len(file_to_substr),
    len(substr_to_file),
    now - start
  ), file=sys.stderr)
  start = now
  if top_k != 0:
    substrCounts = {}
    for substr in substr_to_file:
      substrCounts[substr] = len(substr_to_file[substr])
    topKSubstrs = sortedHist(substrCounts, 1)[:top_k]
    now = time.time()
    print("[+] Done sorting %d substr occurrences; time elapsed: %1.3f" % (len(topKSubstrs), now - start), file=sys.stderr)
  else:
    topKSubstrs = None
    start = now
  # TODO use "indexed" substr occurrences
  cooccurrences = genericCooccurr(file_to_substr, substr_to_file, sampling_rate, top_k, topKSubstrs)
  now = time.time()
  print("[+] Reading %d co-occurrences; time elapsed: %1.3f" % (len(cooccurrences), now - start), file=sys.stderr)
  start = now
  cooccurrence_counts = {}
  for cooccur in cooccurrences:
    cooccurrence_counts[cooccur] = len(cooccurrences[cooccur])
  # TODO dont use a constant
  # TODO really dont use a constant
  if sampling_rate != 0:
    cooccur_sorted_hist = sortedHist(cooccurrence_counts, 1)
  elif top_k != 0:
    cooccur_sorted_hist = sortedHist(cooccurrence_counts, 1)
  else:
    cooccur_sorted_hist = sortedHist(cooccurrence_counts, 10)
  now = time.time()
  print("[+] Done sorting %d co-occurrences; time elapsed: %1.3f" % (len(cooccur_sorted_hist), now - start), file=sys.stderr)
  start = now
  if not tabular:
    print(json.dumps(cooccur_sorted_hist[:20]))
  else:
    for i in range(len(cooccur_sorted_hist)):
      print("%s\t%s\t%d" % (
        cooccur_sorted_hist[i][0][0],
        cooccur_sorted_hist[i][0][1],
        cooccur_sorted_hist[i][1]
      ))

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description='Approximates co-occurences amongst long common substrings'
  )
  parser.add_argument(
    'input_db',
    help='Where the tabular db is stored'
  )
  parser.add_argument(
    '-t',
    '--tabular',
    action='store_true'
  )
  parser.add_argument(
    '-s',
    '--samplingrate',
    type=int
  )
  parser.add_argument(
    '-k',
    '--topk',
    type=int
  )
  args = parser.parse_args()
  sampling_rate = args.samplingrate if args.samplingrate is not None else 0
  top_k = args.topk if args.topk is not None else 0
  if sampling_rate != 0 and top_k != 0:
    print("Cant have both sampling_rate (%d) and top k (%d)" % (sampling_rate, top_k), file=sys.stderr)
    sys.exit(-1)
  main(args.input_db, args.tabular, sampling_rate, top_k)
