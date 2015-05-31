# histogram.py
# Trevor Pottinger
# Sat May 30 22:11:13 PDT 2015

from __future__ import print_function

import argparse
import re
import sys

def main(n):
  ns = []
  regex = re.compile('\d+')
  # read in the data
  for line in sys.stdin:
    match = regex.search(line)
    if match is None:
      continue
    ns.append(int(match.group()))
  # is this necessary?..
  ns.sort(reverse=True)
  num_ns = len(ns)
  # print the results
  # once again annoyed by lack of real for loops
  for i in range(n):
    index = float(i) * (float(num_ns) / n)
    print("%.3f\t%d" % (100.0 - (index / num_ns) * 100.0, ns[int(index)]))
  # print n+1
  print("%.3f\t%d" % (0.000, ns[-1]))

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description='Returns a histogram of integers, expects already sorted'
  )
  parser.add_argument('-n', type=int, default=10)
  args = parser.parse_args()
  main(args.n)
