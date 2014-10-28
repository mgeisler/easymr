
Simple Map-Reduce Framework for ZeroCloud
=========================================

This little framework makes it easier to write map-reduce programs for
ZeroCloud. It takes care of moving data between the mappers and
reducers, you take care of the mapping and reducing logic only.

Word Count Example
------------------

To do a simple word count, you write a `job.py` file with a `mapper`
and a `reducer` function:

```python
import os
import math

def mapper(record):
    path = os.environ['LOCAL_PATH_INFO']
    basename = os.path.basename(path)
    data = record['data']

    return {'path': basename,
            'count': len(data.split())}

def reducer(fp, entries):
    counts = []
    paths = []

    for entry in entries:
        counts.append(entry['count'])
        paths.append(entry['path'])

    size = int(math.log10(max(counts))) + 2
    for count, path in zip(counts, paths):
        fp.write('%*d %s\n' % (size, count, path))
    fp.write('%*d total\n' % (size, sum(counts)))
```

The `mapper` function transforms a `dict`, and the `reducer` function
receives a file pointer to write to and an iterator over the results
from `mapper`.

You execute the scripts with:

```bash
$ ./easymr.py job.py 'swift://~/data/*.txt'
```

This will create a zapp, deploy it on ZeroCloud, execute it on the
Swift inputs given, and finally clean up the whole thing. The result
(the data written to `fp` above) is shown in your terminal.
