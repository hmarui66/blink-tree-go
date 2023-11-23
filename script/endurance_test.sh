#!/bin/bash

set -eox pipefail

cd $(dirname $0)/..

for i in {1..1000}; do
  go test -run TestBLTree_deleteManyConcurrently > data/endurance_test_result.txt
  if [ $? -ne 0 ]; then
    exit 1
  fi
done