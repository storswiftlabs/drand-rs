#!/bin/bash
source $PWD/src/test/interop/scripts/config.sh

$DRAND dkg generate-proposal --id default \
--joiner $ADDRESS \
--joiner $ADDRESS_RS \
--control $CONTROL \
--out $BASE/proposal.toml