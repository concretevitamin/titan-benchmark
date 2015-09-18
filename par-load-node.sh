#!/bin/bash

nodeSplits=( FIXME FIXME )

export MAVEN_OPTS="-server -Xmx15g"

i=0
for split in "${nodeSplits[@]}"; do
    mvn exec:java \
      -Dexec.mainClass="edu.berkeley.cs.benchmark.ParLoadNode" \
      -Dexec.args="${split}" \
      2>&1 >log${i}.log &
    i=$((i + 1))
done
wait
