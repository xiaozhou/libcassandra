#!/usr/bin/env python

import sys
import os
import subprocess

if len(sys.argv) != 3:
    print "Usage: generate-cassandra-thrift.py /path/to/bin/thrift cassandra-version"
    sys.exit(-1)

thrift_bin = sys.argv[1]
cassandra_version = sys.argv[2]

# Sanity check our location
cur_dir_list = os.listdir( os.getcwd() )
if 'libcassandra' not in cur_dir_list or 'libgenthrift' not in cur_dir_list:
    print "Run from the top-level libcassandra directory"
    sys.exit(-1)

# Checkout / update cassandra checkout
if 'cassandra.git' not in cur_dir_list:
    subprocess.call(['git', 'clone', 'git://git.apache.org/cassandra.git', 'cassandra.git'])
subprocess.call(['git', 'fetch', 'origin'], cwd='cassandra.git')
subprocess.call(['git', 'checkout', 'trunk'], cwd='cassandra.git')
subprocess.call(['git', 'branch', '-D', 'libcassandra'], cwd='cassandra.git')
subprocess.call(['git', 'checkout', '-b', 'libcassandra', 'cassandra-' + cassandra_version], cwd='cassandra.git')

# Run thrift
subprocess.call([thrift_bin, '--gen', 'cpp', 'cassandra.git/interface/cassandra.thrift'])

# This leaves the generated content in gen-cpp
subprocess.call(['/bin/bash', '-x', '-c', 'cp gen-cpp/* libgenthrift/'])

# There's an inconveniently named VERSION in cassandra_constants.h and
# cassandra_constants.cpp. These only affect the build of the library (they
# aren't included by any public headers), so we just need to undef VERSION for
# them.
subprocess.call(['cp', 'libgenthrift/cassandra_constants.h', 'libgenthrift/cassandra_constants.h.orig'])
subprocess.call(['sed', 's/std::string VERSION/#undef VERSION\\nstd::string VERSION/', 'libgenthrift/cassandra_constants.h.orig'], stdout=open('libgenthrift/cassandra_constants.h', 'w'))
