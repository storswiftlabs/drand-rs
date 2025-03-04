#!/bin/bash
# WARN: scripts are configured to be executed from rust code, see ../start_dkg.rs

# Drand v2.1.0-insecure (commit 66d4fa4d) Golang binary
# Recompile: git clone https://github.com/drand/drand.git && cd drand && git checkout v2.1.0 && make build_insecure
DRAND="$PWD/src/test/interop/drand"

# Absolute path to base folder
BASE="$PWD/src/test/interop/drand_test"

# Control port
CONTROL="55555"

# Node address
ADDRESS="127.0.0.1:11111"