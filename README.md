# mmlcs
Multi multi longest common substring, or something that sounds like that.

# Background
[Longest Common Substring](http://en.wikipedia.org/wiki/Longest_common_substring_problem)
is a well known problem within Computer Science. It's defined as taking two
strings as input and returns the one, single longest substring that both strings
contain. Multi Longest Common Substring is defined as having two or more
strings and returning the one, single longest substring that is contained
within all the inputs. In MMLCS, we have two or more input strings and
return many substrings that are contained in two or more of the inputs.

# Current State

To currently do anything fun, I recommend starting with mmlcs.py, which has
several supported CLI arguments. `python mmlcs.py -h` is a good way to get
started.

Running `python mmlcs.py <input_dir> -o <output_file> -f tsv` will calculate
a bunch of common substrings, which will get written to the specified output
file and how many files that substring showed up in.

# Future

There is a couple missing abstractions that I'd like to add. The first being
a hash based (such as md5 or sha256), local database of samples and substrings.
Essentially a table between hash and content. The second being a hash based
mapping between sample hashes, substring hashes, and starting index for that
substring. A stretch would to have a table between hash and metadata, such
as content length and maybe other hash types of the same content.

# Usage

`python mmlcs.py /input_dir/ -m -n 3 -t -o tmp_hex_db -c substr_content_dir`

`cut -f2 tmp_hex_db | sort | uniq -c | sort -nr | python histogram.py -n40`

`python yaragen.py -k 20 tmp_hex_db --gen -c substr_content_dir`
