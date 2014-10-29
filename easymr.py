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
    kwargs['stdout'] = subprocess.PIPE
    kwargs['stderr'] = subprocess.PIPE

    try:
        proc = subprocess.Popen(args, **kwargs)
    except OSError:
        print '*** could not find %s in your PATH' % args[0]
        sys.exit(1)

    (stdout, stderr) = proc.communicate()
    if proc.returncode != 0:
        print '*** executing %s failed:' % args[0]
        print stdout
        print stderr
        sys.exit(1)
    return stdout


def main(argv):
    desc = "Run a ZeroCloud map-reduce job"
    parser = argparse.ArgumentParser(argv, description=desc)
    parser.add_argument('job', metavar='JOB', help='the job script')
    parser.add_argument('inputs', metavar='INPUTS', help='input objects')
    args = parser.parse_args()

    if not os.path.exists(args.job):
        parser.error('could not find %s' % args.job)

    root = tempfile.mkdtemp()
    atexit.register(shutil.rmtree, root)
    atexit.register(sys.stdout.write, 'cleaning up temp directory\n')

    print 'creating wrapper zapp in', root
    call('zpm', 'new', root)

    instroot = os.path.dirname(__file__)
    shutil.copyfile(args.job, os.path.join(root, 'job.py'))
    shutil.copy(os.path.join(instroot, 'runner.py'), root)

    zapp_path = os.path.join(root, 'zapp.yaml')
    zapp = yaml.load(open(zapp_path))
    groups = [{'name': 'mapper',
               'path': 'file://python2.7:python',
               'args': 'runner.py map',
               'devices': [{'name': 'python2.7'},
                           {'name': 'input', 'path': args.inputs}],
               'connect': ['reducer']},
              {'name': 'reducer',
               'path': 'file://python2.7:python',
               'args': 'runner.py reduce',
               'devices': [{'name': 'python2.7'},
                           {'name': 'stdout'}]}]
    zapp['meta']['name'] = 'mr'
    zapp['execution']['groups'] = groups
    zapp['bundling'] = ['*.py']
    yaml.dump(zapp, open(zapp_path, 'w'))

    os.chdir(root)
    call('zpm', 'bundle')

    container = 'easymr-tmp-%08d' % random.randrange(100000000)

    print 'deploying to', container
    call('zpm', 'deploy', container, 'mr.zapp')
    atexit.register(call, 'swift', 'delete', container)
    atexit.register(sys.stdout.write, 'cleaning up temp container\n')

    print 'executing job'
    stdout = call('zpm', 'execute', '--container', container, 'mr.zapp')
    sys.stdout.write(stdout)


if __name__ == '__main__':
    try:
        sys.exit(main(sys.argv))
    except KeyboardInterrupt:
        print 'interrupted!'
        sys.exit(1)
