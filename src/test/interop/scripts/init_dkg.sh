#!/bin/bash
source $PWD/src/test/interop/scripts/config.sh

$DRAND dkg init --id AAA \
--period 22s \
--timeout 30s \
--catchup-period 1s \
--genesis-delay 50s \
--threshold 11 \
--scheme pedersen-bls-chained \
--proposal $BASE/proposal.toml \
--control $CONTROL


