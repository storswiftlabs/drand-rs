#!/bin/bash
source $PWD/src/test/interop/scripts/config.sh

$DRAND dkg init --id default \
--period 10s \
--timeout 30s \
--catchup-period 1s \
--genesis-delay 50s \
--threshold 2 \
--scheme pedersen-bls-chained \
--proposal $BASE/proposal.toml \
--control $CONTROL


