#!/bin/bash
source $PWD/src/test/interop/scripts/config.sh

$DRAND dkg generate-proposal --id default \
--joiner $ADDRESS \
--joiner 127.0.0.1:22222 \
--control $CONTROL \
--out $BASE/proposal.toml