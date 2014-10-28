import os
import sys
import json
import traceback

import job


def runmapper():
    """Run mapper over /dev/input."""
    with open('/dev/input') as fp:
        data = fp.read()
    try:
        record = json.loads(data)
    except:
        record = {'data': data}
    mapped = job.mapper(record)
    with open('/dev/out/reducer', 'a') as fp:
        json.dump(mapped, fp)


def runreducer():
    """Read output from mappers and run it through the reducer."""
    def load():
        """Yield sequence of inputs"""
        mappers = '/dev/in'
        for mapper in os.listdir(mappers):
            with open(os.path.join(mappers, mapper)) as fp:
                yield json.load(fp)

    job.reducer(sys.stdout, load())


if __name__ == '__main__':
    try:
        if sys.argv[1] == 'map':
            runmapper()
        else:
            runreducer()
    except Exception, e:
        traceback.print_exc(file=sys.stdout)
