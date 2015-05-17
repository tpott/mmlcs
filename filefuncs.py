# filefuncs.py
# Trevor Pottinger
# Sun May 17 10:08:28 PDT 2015

import multiprocessing

NUM_CORES = multiprocessing.cpu_count()

def simpleFunc(tupleargs):
  filenames = tupleargs[0]
  func = tupleargs[1]
  args = tupleargs[2]
  # RFC are these actually useful?
  raw_lens = []
  extracted_lens = []
  common_extracted = {}
  for filename in filenames:
    # TODO process batch at a time
    blob = open(filename).read()
    # this is returning a dict of <key, count>, should we be using counts?
    hist = func(blob, *args)
    raw_lens.append(len(blob))
    extracted_lens.append(len(hist))
    # this is essentially a second pass over the file...
    for k in hist:
      if k in common_extracted:
        common_extracted[k] += 1
      else:
        common_extracted[k] = 1
    # should we have a debug statement per processed file?
  return (raw_lens, extracted_lens, common_extracted)

def multiFunc(tupleargs):
  filenames = tupleargs[0]
  func = tupleargs[1]
  args = tupleargs[2]
  filename_partitions = []
  partition_size = len(filenames) / NUM_CORES
  for i in range(NUM_CORES):
    filename_partitions.append([
      filenames[i*partition_size:(i+1)*partition_size],
      func,
      args
    ])
  pool = multiprocessing.Pool(NUM_CORES)
  result_partitions = pool.map(simpleFunc, filename_partitions)
  # not sure why these next two lines are necessary
  pool.close()
  pool.join()
  # now to join all the results
  raw_lens = []
  extracted_lens = []
  common_extracted = {}
  for i in range(NUM_CORES):
    raw_lens.extend(result_partitions[i][0])
    extracted_lens.extend(result_partitions[i][1])
    partial_common_extracted = result_partitions[i][2]
    # this is essentially doing another pass over a really big string
    for k in partial_common_extracted:
      if k in common_extracted:
        common_extracted[k] += partial_common_extracted[k]
      else:
        common_extracted[k] = partial_common_extracted[k]
  # does this preserve order of the results?..
  return (raw_lens, extracted_lens, common_extracted)
