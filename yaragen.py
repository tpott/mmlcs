# yaragen.py
# Trevor Pottinger
# Mon May 25 22:12:50 PDT 2015

from __future__ import print_function

# stdlib imports
import argparse
import datetime
import os

from encoding import (bin2hex)

RULE_TEMPLATE = """
rule TODO
{
  meta:
    author = "TODO@fb.com"
    version = "1"
    source = "facebook"
    share_level = "GREEN AMBER RED"
    description = "TODO"
    reference = "TODO"
    sample = "TODO"
    date = "%(ds)s"
    confidence = 10
    severity = 1
  strings:
%(string_list)s
  condition:
    %(num_strings)d of them
}
"""

CHAR_OFFSET = ord('a')
STRING_TEMPLATE = "    $%(identifier)s = {%(hex_content)s}"
TOP_K_DEFAULT = 25

def __hist_cmp(x, y):
  if x[1] > y[1]:
    return 1
  elif x[1] < y[1]:
    return -1
  else:
    return 0

def parseDBFile(filename):
  rows = []
  with open(filename) as f:
    for l in f:
      # strip first, then split
      cols = l.strip().split("\t")
      rows.append({
        'file_hash' : cols[0],
        'substr_hash' : cols[1],
        'offset' : cols[2],
      })
  return rows

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description='Attempts to suggest yara rules'
  )
  parser.add_argument(
    'db_filename',
    help='The name of the file to treat as a DB'
  )
  parser.add_argument(
    '-c',
    '--content',
    help='Where to store the content with filename=hex_hash',
    type=str
  )
  parser.add_argument(
    '-k',
    help='The top K substrs to be used',
    type=int
  )
  parser.add_argument(
    '-g',
    '--gen',
    action='store_true',
    help='Whether or not to generate a yara rule'
  )
  args = parser.parse_args()
  if args.k is not None:
    K = args.k
  else:
    K = TOP_K_DEFAULT
  # read file
  db = parseDBFile(args.db_filename)
  # calculate some stats
  substr_hashes = {}
  for row in db:
    if row['substr_hash'] in substr_hashes:
      substr_hashes[row['substr_hash']] += 1
    else:
      substr_hashes[row['substr_hash']] = 1
  substr_hash_list = substr_hashes.items()
  # this could be slow if theres 1M+ items in the db
  substr_hash_list.sort(__hist_cmp, reverse=True)
  # generate output
  i = 0
  str_conditions = []
  for kv in substr_hash_list[:K]:
    if args.content is None:
      print("%s\t%d" % (kv[0], kv[1]))
      continue
    substr_content_filename = os.path.join(args.content, kv[0])
    # TODO should we verify the hash?
    substr_content = open(substr_content_filename).read()
    if not args.gen:
      print("%s\t%d\t%d" % (kv[0], kv[1], len(substr_content)))
      continue
    # TODO limit the number of substrs
    identifier = chr(CHAR_OFFSET + i)
    str_conditions.append(STRING_TEMPLATE % {
      'identifier' : identifier,
      'hex_content' : bin2hex(substr_content),
    })
    i += 1
  if args.gen:
    print(RULE_TEMPLATE % {
      'ds' : datetime.datetime.now().strftime('%Y-%m-%d'),
      'string_list' : "\n".join(str_conditions),
      'num_strings' : K / 2,
    })
