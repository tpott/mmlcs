# extractors.py
# Trevor Pottinger
# Sun May 17 09:58:11 PDT 2015

def ngrams(data, n):
  "Expects a bytestring and returns a histogram of ngrams"
  assert n >= 0, 'n must be greater than zero: %d' % n
  assert n < len(data), 'n must be less than len(data): %d > %d' % (n, len(data))
  hist = {}
  for i in xrange(len(data) - n + 1):
    # python slicing is the best
    gram = data[i : i + n]
    if gram in hist:
      hist[gram] += 1
    else:
      hist[gram] = 1
  return hist

def substrings(data, n, hist):
  """Assumes hist is a bunch of ngrams. It could be a set or a dict (as long as
  it supports the `in` syntax for membership testing. Although the current
  implementation requires a dict so counts can be constant over a substr"""
  assert n >= 0, 'n must be greater than zero: %d' % n
  assert n < len(data), 'n must be less than len(data): %d > %d' % (n, len(data))
  assert len(hist) > 0, 'hist must be non-empty'
  subs = {}
  i = 0
  while i < len(data) - n + 1:
    gram = data[i : i + n]
    if gram in hist:
      # select longest substring in hist
      end = i + n
      count = hist[gram]
      for j in xrange(i + 1, len(data) - n + 1):
        jgram = data[j : j + n]
        # RFC how should we handle changes in the hist value, aka count for
        #  that ngram? increases are interesting, decreases are unhelpful(?)
        if jgram in hist and hist[jgram] == count:
          end += 1
        else:
          break
      # TODO use a constant instead of 4?
      if end - i >= 4:
        sub = data[i : end]
        if sub in subs:
          subs[sub] += 1
        else:
          subs[sub] = 1
        i = end
    # if only python had real for loops and that bug wouldn't have happened..
    i += 1
  return subs

def ngrams_set_generator(data, n):
  assert n >= 0, 'n must be greater than zero: %d' % n
  assert n < len(data), 'n must be less than len(data): %d > %d' % (n, len(data))
  seen = set()
  for i in xrange(len(data) - n + 1):
    # python slicing is the best
    gram = data[i : i + n]
    if gram in seen:
      continue
    else:
      # TODO this is not a generator, yield `gram` here
      seen.add(gram)
  return seen

def substrings_list(data, n, hist, require_equal_counts=True):
  """Assumes hist is a bunch of ngrams. It could be a set or a dict (as long as
  it supports the `in` syntax for membership testing. Although the current
  implementation requires a dict so counts can be constant over a substr.
  Returns a list of (substring, index)."""
  assert n >= 0, 'n must be greater than zero: %d' % n
  assert n < len(data), 'n must be less than len(data): %d > %d' % (n, len(data))
  assert len(hist) > 0, 'hist must be non-empty'
  subs = []
  seen = set()
  i = 0
  # TODO math.log(len(hist))
  min_file_count = 10
  # RFC does max substring make sense?
  # max_file_count makes sense when <10% samples cluster
  # TODO this probably depends on expected results
  min_substring_len = 8
  while i < len(data) - n + 1:
    gram = data[i : i + n]
    if gram not in hist or hist[gram] <= min_file_count:
      i += 1  # ugh now we increment i in two places.. for loops would fix this
      continue
    # select longest substring in hist
    end = i + n
    count = hist[gram]
    for j in xrange(i + 1, len(data) - n + 1):
      jgram = data[j : j + n]
      # RFC how should we handle changes in the hist value, aka count for
      #  that ngram? increases are interesting, decreases are unhelpful(?)
      #if jgram in hist and hist[jgram] == count:
      # Or does a dynamic threshhold make more sense?
      if jgram in hist and \
          ((not require_equal_counts and hist[jgram] > min_file_count) or \
           (require_equal_counts and hist[jgram] == count)):
        end += 1
      else:
        break
    if end - i > min_substring_len:
      sub = data[i : end]
      if sub not in seen:
        # TODO hash the sub here?
        subs.append( (sub, i) )
        seen.add(sub)
      else:
        # RFC should we include multiple occurances?
        pass
      i = end
    # if only python had real for loops and that bug wouldn't have happened..
    i += 1
  return subs
