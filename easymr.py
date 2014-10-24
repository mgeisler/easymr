#!/usr/bin/env python
#
#  Copyright 2014 Rackspace, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import sys
import yaml
import atexit
import shutil
import random
import argparse
import tempfile
import subprocess


def call(*args, **kwargs):
    verbose = kwargs.pop('verbose', False)
    kwargs['stdout'] = subprocess.PIPE
    kwargs['stderr'] = subprocess.STDOUT

    try:
        proc = subprocess.Popen(args, **kwargs)
    except OSError:
        print '*** could not find %s in your PATH' % args[0]
        sys.exit(1)

    (stdout, stderr) = proc.communicate()
    if proc.returncode != 0:
        print '*** executing %s failed:' % args[0]
        print stdout
        sys.exit(1)
    if verbose:
        print stdout


def output(*msgs):
    msg = ' '.join(map(str, msgs))
    print msg


def main(argv):
    desc = "Run a ZeroCloud map-reduce job"
    parser = argparse.ArgumentParser(argv, description=desc)
    parser.add_argument('mapper', metavar='MAPPER', help='the mapper')
    parser.add_argument('reducer', metavar='REDUCER', help='the reducer')
    parser.add_argument('inputs', metavar='INPUTS', help='input objects')
    args = parser.parse_args()

    if not os.path.exists(args.mapper):
        parser.error('could not find %s' % args.mapper)
    if not os.path.exists(args.reducer):
        parser.error('count not find %s' % args.reducer)

    root = tempfile.mkdtemp()
    atexit.register(shutil.rmtree, root)
    atexit.register(output, 'cleaning up temp directory')

    output('creating wrapper zapp in', root)
    call('zpm', 'new', root)

    instroot = os.path.dirname(__file__)
    shutil.copy(args.mapper, root)
    shutil.copy(args.reducer, root)
    shutil.copy(os.path.join(instroot, 'runmapper.py'), root)
    shutil.copy(os.path.join(instroot, 'runreducer.py'), root)

    zapp_path = os.path.join(root, 'zapp.yaml')
    zapp = yaml.load(open(zapp_path))
    groups = [{'name': 'mapper',
               'path': 'file://python:python',
               'args': 'runmapper.py',
               'devices': [{'name': 'python'},
                           {'name': 'input', 'path': args.inputs}],
               'connect': ['reducer']},
              {'name': 'reducer',
               'path': 'file://python:python',
               'args': 'runreducer.py',
               'devices': [{'name': 'python'},
                           {'name': 'stdout'}]}]
    zapp['meta']['name'] = 'mr'
    zapp['execution']['groups'] = groups
    zapp['bundling'] = ['*.py']
    yaml.dump(zapp, open(zapp_path, 'w'))

    os.chdir(root)
    call('zpm', 'bundle')

    container = 'mr-%d' % random.randrange(1000000)

    output('deploying to', container)
    call('zpm', 'deploy', container, 'mr.zapp')
    atexit.register(call, 'swift', 'delete', container)
    atexit.register(output, 'cleaning up temp container')

    output('executing job')
    call('zpm', 'execute', '--container', container, 'mr.zapp',
         verbose=True)


if __name__ == '__main__':
    try:
        sys.exit(main(sys.argv))
    except KeyboardInterrupt:
        output('interrupted!')
