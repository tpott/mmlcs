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

def main(input_db, tabular):
  num_lines_read = 0
  file_to_substr = {}
  substr_to_file = {}
  start = time.time()
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
  now = time.time()
  print("[+] Reading %d lines, %d file hashes, and %d substr hashes complete; time elapsed: %1.3f" % (
    num_lines_read,
    len(file_to_substr),
    len(substr_to_file),
    now - start
  ))
  start = now
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
  now = time.time()
  print("[+] Reading %d co-occurrences; time elapsed: %1.3f" % (len(cooccurrences), now - start))
  start = now
  cooccurrence_counts = {}
  for cooccur in cooccurrences:
    cooccurrence_counts[cooccur] = len(cooccurrences[cooccur])
  # TODO dont use a constant
  cooccur_sorted_hist = sortedHist(cooccurrence_counts, 30)
  now = time.time()
  print("[+] Done sorting %d co-occurrences; time elapsed: %1.3f" % (len(cooccur_sorted_hist), now - start))
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
    #help='How the output should be formated. TSV or JSON'
  )
  args = parser.parse_args()
  main(args.input_db, args.tabular)
