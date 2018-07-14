#!/bin/bash

#
# Launcher for tag aggregator parsing variable number of command line arguments. Eg.
#
# bash launch.sh SNAPSHOT_LATEST_REVCOUNT /path/to/input.jsonl /path/to/output.jsonl
#
# will run
#
# ./gradlew -PappArgs="['SNAPSHOT_LATEST_REVCOUNT', '/path/to/input.jsonl', '/path/to/output.jsonl']" run
#
# TODO: the below fails if no arguments are given

set -eux

ARGUMENTS="['$1'"
shift

while [[ $# -gt 0 ]]; do
  ARGUMENTS="${ARGUMENTS},'$1'"
  shift
done
ARGUMENTS=${ARGUMENTS}]

./gradlew -PappArgs=$ARGUMENTS run
