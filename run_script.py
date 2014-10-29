#!/usr/bin/env python
"""A handy utility script for running Python scripts in this repo with the
proper jar added to Spark's classpath."""
from glob import glob
import os
import sys


_HERE = os.path.dirname(os.path.abspath(__file__))
def here(*args):
    return os.path.join(_HERE, *args)


def get_latest_local_jar():
    return glob(here("target", "pyspark-cassandra*.jar"))[0]


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: run_script.py <python_script> [script args...]\n")
        sys.exit(-1)

    python_script = sys.argv[1]
    jar = get_latest_local_jar()
    args = ["--driver-class-path",
            jar,
            python_script] + sys.argv[2:]

    # os.execvp uses whatever spark-submit is on the path
    args.insert(0, "spark-submit")
    print " ".join(args)
    os.execvp("spark-submit", args)
