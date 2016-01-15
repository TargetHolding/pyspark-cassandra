#!/usr/bin/env python

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

from setuptools import setup, find_packages


basedir = os.path.dirname(os.path.abspath(__file__))
os.chdir(basedir)

def f(*path):
	return open(os.path.join(basedir, *path))

setup(
	name='pyspark_cassandra',
	maintainer='Frens Jan Rumph',
	maintainer_email='frens.jan.rumph@target-holding.nl',
	version=f('../version.txt').read(),
	description='Utilities to asssist in working with Cassandra and PySpark.',
	long_description=f('../README.md').read(),
	url='https://github.com/TargetHolding/pyspark-cassandra',
	license='Apache License 2.0',

	packages=find_packages(),
	include_package_data=True,

	classifiers=[
		'Development Status :: 2 - Pre-Alpha',
		'Environment :: Other Environment',
		'Framework :: Django',
		'Intended Audience :: Developers',
		'License :: OSI Approved :: Apache Software License',
		'Operating System :: OS Independent',
		'Programming Language :: Python',
		'Programming Language :: Python :: 2',
		'Programming Language :: Python :: 2.7',
		'Topic :: Database',
		'Topic :: Software Development :: Libraries',
		'Topic :: Scientific/Engineering :: Information Analysis',
		'Topic :: Utilities',
	]
)
