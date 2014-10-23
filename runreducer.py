import os
import json

import reducer


def load():
    mappers = '/dev/in'
    for mapper in os.listdir(mappers):
        with open(os.path.join(mappers, mapper)) as fp:
            yield json.load(fp)

if __name__ == '__main__':
    with open('/dev/stdout', 'a') as fp:
        reducer.reducer(fp, load())
