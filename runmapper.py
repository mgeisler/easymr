import json

import mapper

if __name__ == '__main__':
    with open('/dev/input') as fp:
        data = fp.read()
    try:
        record = json.loads(data)
    except:
        record = {'data': data}
    mapped = mapper.mapper(record)
    with open('/dev/out/reducer', 'a') as fp:
        json.dump(mapped, fp)
