#!/bin/bash
source $PWD/src/test/interop/scripts/config.sh

$DRAND generate-keypair \
--folder $BASE \
--control $CONTROL \
--id AAA \
--scheme pedersen-bls-chained \
$ADDRESS > /dev/null 2>&1

$DRAND start --folder $BASE  \
--private-listen $ADDRESS \
--verbose \
--control $CONTROL > $BASE/leader.log 2>&1 &