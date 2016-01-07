SHELL = /bin/bash
VERSION = $(shell grep ^version build.sbt | sed 's/version := //g' | sed 's/"//g')

.PHONY: clean clean-pyc clean-dist dist test-travis



clean: clean-dist clean-pyc

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-dist:
	rm -rf target
	rm -rf python/build/
	rm -rf python/*.egg-info



install-venv:
	test -d venv || virtualenv venv
	
install-cassandra-driver: install-venv
	venv/bin/pip install cassandra-driver
	
install-ccm: install-venv
	venv/bin/pip install ccm

start-cassandra: install-ccm	
	mkdir -p .ccm
	venv/bin/ccm status --config-dir=.ccm || venv/bin/ccm create pyspark_test -v 2.2.3 -n 1 -s --config-dir=.ccm
	
stop-cassandra:
	venv/bin/ccm remove --config-dir=.ccm



test: test-python test-scala test-integration

test-python:

test-scala:

test-integration: \
	test-integration-setup \
	test-integration-matrix \	
	test-integration-teardown
	
test-integration-setup: \
	start-cassandra

test-integration-teardown: \
	stop-cassandra
	
test-integration-matrix: \
	install-cassandra-driver \
	test-integration-spark-1.4.1 \
	test-integration-spark-1.5.0 \
	test-integration-spark-1.5.1 \
	test-integration-spark-1.5.2

test-travis:
	$(call test-integration-for-version,$$SPARK_VERSION,$$SPARK_PACKAGE_TYPE)

test-integration-spark-1.3.1:
	$(call test-integration-for-version,1.3.1,hadoop2.6)

test-integration-spark-1.4.1:
	$(call test-integration-for-version,1.4.1,hadoop2.6)

test-integration-spark-1.5.0:
	$(call test-integration-for-version,1.5.0,hadoop2.6)

test-integration-spark-1.5.1:
	$(call test-integration-for-version,1.5.1,hadoop2.6)

test-integration-spark-1.5.2:
	$(call test-integration-for-version,1.5.2,hadoop2.6)

define test-integration-for-version
	echo ======================================================================
	echo testing integration with spark-$1
	
	mkdir -p lib && test -d lib/spark-$1-bin-$2 || \
		(pushd lib && curl http://ftp.tudelft.nl/apache/spark/spark-$1/spark-$1-bin-$2.tgz | tar xz && popd)
	
	cp log4j.properties lib/spark-$1-bin-$2/conf/

	source venv/bin/activate ; \
		lib/spark-$1-bin-$2/bin/spark-submit \
			--master local[*] \
			--driver-memory 512m \
			--conf spark.cassandra.connection.host="localhost" \
			--jars target/scala-2.10/pyspark-cassandra-assembly-$(VERSION).jar \
			--py-files target/pyspark_cassandra-$(VERSION)-py2.7.egg \
			python/pyspark_cassandra/tests.py
			
	echo ======================================================================
endef



dist: dist-python dist-scala

dist-python:
	python/setup.py bdist_egg -d ../target
	rm -rf python/build/
	rm -rf python/*.egg-info

dist-scala:
	sbt compile assembly


all: clean dist
