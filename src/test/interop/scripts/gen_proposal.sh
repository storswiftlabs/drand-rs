#!/bin/bash
source "$PWD/src/test/interop/scripts/config.sh"

JOINERS=""

for NODE in "$@"; do
    JOINERS+="--joiner $NODE "
done

$DRAND dkg generate-proposal --id AAA \
--joiner "$ADDRESS" \
$JOINERS \
--control "$CONTROL" \
--out "$BASE/proposal.toml"