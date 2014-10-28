import os
import sys
import json
import traceback

import reducer


def load():
    mappers = '/dev/in'
    for mapper in os.listdir(mappers):
        with open(os.path.join(mappers, mapper)) as fp:
            yield json.load(fp)

if __name__ == '__main__':
    try:
        reducer.reducer(sys.stdout, load())
    except Exception, e:
        traceback.print_exc(file=sys.stdout)
