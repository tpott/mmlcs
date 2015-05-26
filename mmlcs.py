# mmlcs.py
# Trevor Pottinger
# Tue May 12 22:56:34 PDT 2015

from __future__ import print_function

# stdlib imports
import argparse
import glob
import hashlib
import json
import multiprocessing
import os
import struct
import sys
import time

# local imports
from extractors import (ngrams, substrings)
from extractors import (ngrams_set_generator, substrings_list)
from filefuncs import (simpleFunc, multiFunc)
from filefuncs import (hashedFunc, hashedMultiFunc)
from sorting import (mergeSort, multiMergeSort)

DEBUG = False
ENABLE_MULTICORE = True
HASH_FUNC = 'md5'
NUM_CORES = multiprocessing.cpu_count()
NGRAMS_DEFAULT = 3
OUTPUT_FORMAT_DEFAULT = 'tsv'

def __hist_cmp(x, y):
  if x[1] > y[1]:
    return 1
  elif x[1] < y[1]:
    return -1
  else:
    return 0

def __substr_hist_cmp(x, y):
  # how many occurances
  if x[1] > y[1]:
    return 1
  elif x[1] < y[1]:
    return -1
  else:
    # length of substrs
    if len(x[0]) > len(y[0]):
      return 1
    elif len(x[0]) < len(y[0]):
      return -1
    else:
      return 0

def sortedHist(hist, minT=0):
  "Actually returns a sorted list of (key, value) tuples"
  tuples = hist.items()
  if minT > 0:
    tuples = filter(lambda kvtuple: kvtuple[1] > minT, tuples)
  # True implies reverse=True, aka DESCENDING
  return mergeSort(tuples, __hist_cmp, True)

def sortedSubstrHist(hist, minT=0):
  tuples = hist.items()
  if minT > 0:
    tuples = filter(lambda kvtuple: kvtuple[1] > minT, tuples)
  # True implies reverse=True, aka DESCENDING
  return mergeSort(tuples, __substr_hist_cmp, True)

def multiSortedHist(hist, minT=0):
  "This seems to be memory bound :("
  tuples = hist.items()
  if minT > 0:
    tuples = filter(lambda kvtuple: kvtuple[1] > minT, tuples)
  # True implies reverse=True, aka DESCENDING
  return multiMergeSort(tuples, __hist_cmp, True)

def bin2hex(s):
  return ''.join( ("%02x" % ord(c) for c in s) )

def hex2bin(s):
  binstr = ''
  for i in range(0, len(s), 2):
    # don't use += to be explicit about ordering
    binstr = binstr + struct.pack('B', int(s[i:i+2], 16))
  return binstr

def main(path_regex, outfile, outformat, use_multi, N, verbosity):
  start = time.time()
  filenames = glob.glob(path_regex)
  print("Running mmlcs on %d files using %d cores looking for %d-grams" % (
    len(filenames),
    NUM_CORES if use_multi else 1,
    N
  ))
  # TODO we could probably select a set instead of a histogram per file
  if not use_multi:
    (_, _, common_ngrams) = simpleFunc(
      (filenames, ngrams, [N])
    )
  else:
    (_, _, common_ngrams) = multiFunc(
      (filenames, ngrams, [N])
    )
  now = time.time()
  print("[+] Reading %d files complete; time elapsed: %1.3f" % (len(filenames), now - start))
  start = now
  # note that the following functions currently take a histogram and return
  #  a sorted list of (ngram, count) tuples
  if not use_multi or True:
    # SLOW because common_ngrams gets huge
    sorted_common_ngrams = sortedHist(common_ngrams, 1)
  else:
    # multi core sorting doesn't work yet...
    sorted_common_ngrams = multiSortedHist(common_ngrams, 1)
  now = time.time()
  print("[+] Sorting %d ngrams complete; time elapsed: %1.3f" % (len(sorted_common_ngrams), now - start))
  start = now
  # RFC does top 25% make sense?
  top_k_index = len(sorted_common_ngrams) / 4
  top_k = dict(sorted_common_ngrams[:top_k_index])
  if not use_multi:
    # RFC we're ignoring the count of distinct substrings
    (_, _, common_substrings) = simpleFunc(
      (filenames, substrings, [N, top_k])
    )
  else:
    (_, _, common_substrings) = multiFunc(
      (filenames, substrings, [N, top_k])
    )
  now = time.time()
  print("[+] Extracting %d substrings complete; time elapsed: %1.3f" % (len(common_substrings), now - start))
  start = now
  if not use_multi or True:
    # This shouldn't be too slow since its sample size is much smaller than above
    # Note that this returns a sorted list of (substring, count) tuples
    sorted_common_substrings = sortedSubstrHist(common_substrings, 1)
  else:
    # TODO multicore substr sorting
    sorted_common_substrings = sortedSubstrHist(common_substrings, 1)
  now = time.time()
  print("[+] Sorting %d substrings complete; time elapsed: %1.3f" % (len(sorted_common_substrings), now - start))
  start = now
  pretty_common_substrings_raw = map(
    lambda kvtuple: (bin2hex(kvtuple[0]), kvtuple[1]),
    sorted_common_substrings
  )
  if outfile is not None:
    assert outformat is not None, 'outformat should never be None'
    with open(outfile, 'w') as f:
      print("[+] Writing %d substrings and counts to %s" % (len(sorted_common_substrings), outfile))
      if outformat == 'json':
        f.write("%s\n" % pretty_common_substrings_raw)
      elif outformat == 'tsv':
        for kvtuple in pretty_common_substrings_raw:
          f.write("%s\t%s\n" % (str(kvtuple[1]), kvtuple[0]))
      else:
        print("Unknown output format %s" % outformat)
  else:
    # no stored output, so lets print some stuff to stdout
    print("Count\tLength\tPreview")
    # TODO allow for more than the top 10 common substrings?
    for kvtuple in pretty_common_substrings_raw[:10]:
      # note: divide length by 2 since it's hex..
      # TODO allow for different preview lengths?
      print("%d\t%d\t%s" % (kvtuple[1], len(kvtuple[0]) / 2, kvtuple[0][:30]))
  return

def main2(path_regex, outfile, outformat, use_multi, N, verbosity, content_output):
  start = time.time()
  filenames = glob.glob(path_regex)
  print("Running mmlcs on %d files using %d cores looking for %d-grams" % (
    len(filenames),
    NUM_CORES if use_multi else 1,
    N
  ))
  # SELECT ngram, COUNT(DISTINCT file)
  # GROUP BY ngram
  if not use_multi:
    (_, _, common_ngrams) = simpleFunc(
      (filenames, ngrams_set_generator, [N])
    )
  else:
    (_, _, common_ngrams) = multiFunc(
      (filenames, ngrams_set_generator, [N])
    )
  now = time.time()
  print("[+] Reading %d files complete; time elapsed: %1.3f" % (len(filenames), now - start))
  start = now
  # note that the following functions currently take a histogram and return
  #  a sorted list of (ngram, count) tuples
  # WHERE COUNT > 1
  # ORDER BY COUNT DESC
  if not use_multi or True:
    # SLOW because common_ngrams gets huge
    sorted_common_ngrams = sortedHist(common_ngrams, 1)
  else:
    # multi core sorting doesn't work yet...
    sorted_common_ngrams = multiSortedHist(common_ngrams, 1)
  now = time.time()
  print("[+] Sorting %d ngrams complete; time elapsed: %1.3f" % (len(sorted_common_ngrams), now - start))
  start = now
  # RFC does top 25% make sense?
  top_k_index = len(sorted_common_ngrams) / 4
  top_k = dict(sorted_common_ngrams[:top_k_index])
  if not use_multi:
    (substr_content, substr_occurances) = hashedFunc(
      (filenames, substrings_list, HASH_FUNC, [N, top_k])
    )
  else:
    (substr_content, substr_occurances) = hashedMultiFunc(
      (filenames, substrings_list, HASH_FUNC, [N, top_k])
    )
  now = time.time()
  print("[+] Extracting %d substrings complete; time elapsed: %1.3f" % (len(substr_content), now - start))
  start = now
  if content_output is not None:
    print("[+] Writing %d substrings content to %s" % (len(substr_content), content_output))
    for hash_key in substr_content:
      filename = os.path.join(
        content_output,
        hash_key
      )
      if os.path.isfile(filename):
        # TODO verify hash?
        # hashlib.new('md5', open(hash).read()).hexdigest() == hash
        continue
      with open(filename, 'w') as f:
        f.write(substr_content[hash_key])
  if outfile is not None:
    assert outformat is not None, 'outformat should never be None'
    with open(outfile, 'w') as f:
      print("[+] Writing %d substring occurances to %s" % (len(substr_occurances), outfile))
      if outformat == 'json':
        print('json tabular output not yet supported')
      elif outformat == 'tsv':
        # list<tuple<file hash, content hash, index>>
        for tup in substr_occurances:
          # Note that the hashes are hex
          f.write("%s\t%s\t%d\n" % tup)
      else:
        print("Unknown output format %s" % outformat)
  else:
    print('No output file was specified')

def validateInput(args):
  # input directory
  if args.input_dir is None:
    raise Exception('input_dir is needed')
  elif not os.path.isdir(args.input_dir):
    raise Exception("%s is not a directory" % args.input_dir)
  else:
    input_dir = os.path.join(args.input_dir, '*')
  # output file
  if args.output is not None and not os.path.exists(args.output):
    output = args.output
  elif args.output is not None and os.path.isdir(args.output):
    raise Exception("%s is a directory, can't write to it" % args.output)
  elif args.output is None:
    output = None
  else:
    # TODO verify output file is writable
    print("[-] WARNING: Don't know what to do with %s... #f*%kit #doitlive" % args.output)
    output = args.output
  # output format
  if args.format is None:
    output_format = OUTPUT_FORMAT_DEFAULT
  elif args.format.lower() == 'json':
    output_format = 'json'
  elif args.format.lower() == 'tsv':
    output_format = 'tsv'
  else:
    print("[-] WARNING: Unknown output format %s, assuming json" % args.format)
    output_format = OUTPUT_FORMAT_DEFAULT
  # verbosity
  if args.verbose is None:
    verbosity = 0
  else:
    # action='count' implies the value will be an integer
    verbosity = args.verbose
  if args.n is None:
    N = NGRAMS_DEFAULT
  else:
    N = args.n
  if args.content is not None:
    if not args.tabular:
      print('You specified a content output dir, but not running in tabular mode')
    if not os.path.isdir(args.content):
      raise Exception("%s is not a directory" % args.input_dir)
    content_output = args.content
  else:
    content_output = None
  return (
      input_dir,
      output,
      output_format,
      args.multi,
      N,
      verbosity,
      args.tabular,
      content_output
      )

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
  parser.add_argument(
    '-m',
    '--multi',
    action='store_true',
    help='Toggles whether or not to use multiple cores'
  )
  parser.add_argument(
    '-t',
    '--tabular',
    action='store_true',
    help='Extract tabular substrings'
  )
  parser.add_argument(
    '-c',
    '--content',
    help='Where to store the content with filename=hex_hash'
  )
  parser.add_argument('-n', help='The value of n for n-grams', type=int)
  parser.add_argument('-v', '--verbose', action='count')
  (input_dir_regex,
   output,
   outformat,
   use_multi,
   n,
   verbosity,
   tabular,
   content_output
   ) = validateInput(
    parser.parse_args()
  )
  if not tabular:
    main(input_dir_regex, output, outformat, use_multi, n, verbosity)
  else:
    main2(input_dir_regex, output, outformat, use_multi, n, verbosity, content_output)
