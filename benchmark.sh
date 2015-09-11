#!/bin/bash
set -e

dataset=livejournal
#testMain=edu.berkeley.cs.benchmark.Benchmark
testMain=edu.berkeley.cs.benchmark.tao.BenchTao
latencyOrThroughput=latency
QUERY_DIR=/mnt/liveJournal-40attr16each-queries
OUTPUT_DIR=/mnt/livejournal_output


primitive_tests=(
  Neighbor
  NeighborNode
  NeighborAtype
  Node
  NodeNode
  Mix
)

tao_tests=(
  AssocRange
  ObjGet
  AssocGet
  AssocCount
  AssocTimeRange
  Mix
)

if [[ $testMain == *"Tao"* ]]
then
  tests=$tao_tests
else
  tests=$primitive_tests
fi

#JVM_HEAP=6900
#echo "Setting -Xmx to ${JVM_HEAP}m"
export MAVEN_OPTS="-Xmx102400M"

warmup=100000
measure=100000

for test in "${tests[@]}"
do
  sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
  mvn exec:java -Dexec.mainClass="${testMain}" \
    -Dexec.args="${test} ${latencyOrThroughput} ${dataset} ${QUERY_DIR} ${OUTPUT_DIR} ${warmup} ${measure}"
  #java -verbose:gc -Xmx${JVM_HEAP}m -cp ${classpath} \
  #   edu.berkeley.cs.benchmark.BenchTao ${test} \
  #   latency \
  #   ${dataset} \
  #   ${QUERY_DIR} \
  #   ${OUTPUT_DIR} \
  #   ${warmup} \
  #   ${measure} \
done
