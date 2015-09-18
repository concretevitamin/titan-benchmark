#!/bin/bash

nodeSplits=( FIXME FIXME )

export MAVEN_OPTS="-server -Xmx100g"

mvn exec:java \
  -Dexec.mainClass="edu.berkeley.cs.benchmark.ParLoadNode" \
  -Dexec.args="${nodeSplits[*]}" \
  2>&1 >parNode.log
