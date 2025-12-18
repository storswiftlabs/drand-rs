This runbook is specific to Drand implementation in Rust and assumes operational experience with Drand ecosystem. If you simply want to run some demo - see [README](./README.md#make-demo).  

**Disclaimer**: Tested on x86_64 Ubuntu 20.04 and 22.04.

## Safety
During setup, we work with copied Drand-go materials. No modifications are required for original files.

## Prerequisites
* You are a member of LoE Testnet or running some DevNet (access to the Daemon Go is required).
* Protocol Buffers compiler is installed: `sudo apt-get install -y protobuf-compiler` 

## Case 1: Faster randomness delivery from node for relay providers.

### Build
Compile `drand-rs` with custom flags:
- `insecure` - (noTLS mode) to connect Drand-go locally for syncing chain database.
- `skip_db_verification` - (optional) to bypass expensive chain checks during sync.

```bash
$ git clone https://github.com/storswiftlabs/drand-rs.git
$ cd drand-rs
$ cargo build --no-default-features --features asm,insecure,skip_db_verification --release
$ cd target/release && mv drand drand_rs # && copy to your preferred location.

# Check version
$ drand_rs --version
# it should say: drand-rs.beta 0.2.1 [asm] insecure skip-db-verification
```

### Create filesystem for Drand-rs and sync chain data from Drand-go.

1. Generate keypairs using the same schemes and `beacon_ids` as in running Drand-go instance. At this stage you need to use different from Drand-go `private-listen` address and `control` port.

```bash
# Run for every network (id). 
$ drand_rs generate-keypair --id <beacon_id> --folder <base/folder/rs> --scheme <scheme/from/go> <host:port_rs> --control <control_port_rs> 
```

2. Start Drand-rs.
```bash
$ drand_rs start --verbose --folder <base_folder_rs> --private-listen <host:port_rs> --control <control_port_rs>
```

3. Get `chain-hash` from running Drand-go for each network (you can also use implementation in Rust to communicate with Go Daemon).
```bash
$ drand_rs show chain-info --id <beacon_id> --control <control_port_GO>
```

4. Fetch chain data for each network up to a recent round.
```bash
$ drand_rs sync --follow --chain-hash <chain_hash_from_step_3> --id <beacon_id> --sync-nodes <private_listen_address_go> --control <control_port_rs>
```

5. After chain data is synced - copy identity and distributed materials from Drand Go.  
This can be done manually or using `rsync`.
```bash
# Stop Drand-rs.
$ drand_rs stop --control <control_port_rs>

# Sync all files except dababase and DKG state machine.
rsync -av --exclude='db/*' --exclude='dkg.db' <base_folder_go>/ <base_folder_rs>/
```

6. Now we have valid filesystem for Drand-rs which is equial to Drand-go identity.
```bash
# Stop Daemon Go
$ drand_go stop --control <control_port_go>

# Start Drand-rs using same command as for Drand-go. Verify beacon discrepancy metrics.
```

## Case 2: Optimized sync
1. Rebuild binary without `skip_db_verification` flag. Remove flag `insecure` if you are connecting over TLS.
```bash
cargo build --no-default-features --features asm --release
```

2. Repeat steps 1-4 from **Case 1** with both implementation for arbitrary sync duration.

Please report any issues or suggestions!
