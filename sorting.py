# sorting.py
# Trevor Pottinger
# Sun May 17 20:08:57 PDT 2015

# Inspired by
# https://devopslog.wordpress.com/2012/04/15/mergesort-example-using-python-multiprocessing/

from __future__ import print_function

import multiprocessing

NUM_CORES = multiprocessing.cpu_count()

# If only these could be an enum
ASCENDING = False
DESCENDING = True

def _merge(a, b, cmp, order):
  """Returns a single sorted list, derived from two sorted lists, a cmp
  function to compare items in the input lists, and a boolean to decide
  between order descending or ascending (the default for list.sort())."""
  ret = []
  # is taking the len many times bad?..
  while len(a) > 0 or len(b) > 0:
    if len(a) > 0 and len(b) > 0:
      # Always compare from index zero
      if not (cmp(a[0], b[0]) > 0) ^ order:
        # is _merge pass by reference? do we mutate a?
        ret.append(a.pop(0))
      else:
        ret.append(b.pop(0))
    elif len(a) > 0:  # len(b) == 0
      ret.append(a.pop(0))
    elif len(b) > 0:  # len(a) == 0
      ret.append(b.pop(0))
    else:
      assert False and len(a) == 0 and len(b) == 0, 'Impossible: %d %d' % (len(a), len(b))
  # assert len(ret) == orig_len_a + orig_len_b
  return ret

def mergeSort(l, cmp, order=DESCENDING, depth=0):
  """Returns a new list that is a sorted list of l. Python's sort by default
  uses reverse=False to imply that the ordering is ascending. The default
  here is to be descending."""
  assert l is not None, 'Cant sort None, try an empty list'
  if len(l) <= 1:
    return l
  # This is a major cheat
  copy = list(l)
  copy.sort(cmp=cmp, reverse=order)
  return copy
  # This is a cheat to improve memory consumption.. Maybe
  if depth > 5:
    copy = list(l)
    copy.sort(cmp=cmp, reverse=order)
    return copy
  elif depth == 0:
    print("Warning: mergeSort seems to be slow...")
  mid = int(len(l) / 2)
  # these could be done in parallel..
  a = mergeSort(l[:mid], cmp, order, depth+1)
  b = mergeSort(l[mid:], cmp, order, depth+1)
  return _merge(a, b, cmp, order)

def _mergeSort(args):
  return mergeSort(args[0], args[1], args[2])

def multiMergeSort(l, cmp, order=DESCENDING):
  partitions = []
  partition_size = len(l) / NUM_CORES
  # off by one fix dependent on python's slicing
  for i in range(NUM_CORES+1):
    partitions.append(
      [l[i * partition_size : (i+1) * partition_size], cmp, order]
    )
  pool = multiprocessing.Pool(NUM_CORES)
  partial_results = pool.map(_mergeSort, partitions)
  # why are we doing these?
  pool.close()
  pool.join()
  ret = []
  for i in range(len(partial_results)):
    # do we need to access _merge() here?
    ret = _merge(ret, partial_results[i], cmp, order)
  assert len(l) == len(ret), 'len of input %d should be equal to output %d' % (len(l), len(ret))
  return ret
