use std::str::FromStr;

use drand::{
    protobuf::drand::{Metadata, StatusRequest, SyncRequest},
    store::{rocksdb::RocksStore, Store},
};
use tonic::transport::{Channel, Uri};

extern crate drand;

#[tokio::main]
async fn main() {
    let cmd = clap::Command::new("drand-client")
        .args([clap::Arg::new("address")
            .long("address")
            .help("node address")])
        .subcommand(
            clap::Command::new("get").args([
                clap::Arg::new("round").long("round").help("round number"),
                clap::Arg::new("beacon")
                    .long("beacon")
                    .help("beacon id")
                    .default_value("evmnet-t"),
                clap::Arg::new("path")
                    .long("path")
                    .help("data dir")
                    .default_value("datastore/example_rocksdb"),
            ]),
        );

    let matches = cmd.get_matches();
    match matches.subcommand() {
        Some(("get", matches)) => {
            let path = matches.get_one::<String>("path").unwrap();
            let beacon_id = matches.get_one::<String>("beacon").unwrap();
            let round = matches.get_one::<String>("round").unwrap();
            let round = round.parse::<u64>().unwrap();
            get_beacon(path, beacon_id, round).await;
        }
        _ => {
            let address = matches.get_one::<String>("address").unwrap();
            client(&address).await;
        }
    };
}

async fn get_beacon(path: &str, beacon_id: &str, round: u64) {
    let store =
        RocksStore::new(path.into(), true, beacon_id.to_owned()).expect("Failed to create store");
    let beacon = match round {
        0 => store.last().await,
        _ => store.get(round).await,
    };
    match beacon {
        Ok(beacon) => {
            println!("Beacon found");
            println!("round {}", beacon.round);
            println!("signature {}", hex::encode(beacon.signature));
            println!("previous_sig {}", hex::encode(beacon.previous_sig));
        }
        Err(e) => {
            println!("Error: {}", e);
        }
    };
}

/// Example with min
async fn client(address: &str) {
    let path = "datastore/example_rocksdb";

    println!("Creating store at {}", path);

    use drand::protobuf::drand::protocol_client::ProtocolClient;
    use tokio_stream::StreamExt;
    let uri = Uri::from_str(&address).unwrap();
    let channel = Channel::builder(uri).connect().await.unwrap();
    let mut client = ProtocolClient::new(channel);
    let id = "evmnet-t";

    let request = StatusRequest {
        check_conn: Vec::new(),
        metadata: Some(Metadata {
            node_version: None,
            beacon_id: id.to_owned(),
            chain_hash: vec![],
        }),
    };

    let status = client.status(request).await.unwrap();

    let store = RocksStore::new(path.into(), true, id.to_owned()).expect("Failed to create store");

    let last_round = status.into_inner().chain_store.unwrap().last_stored;

    let start_round = match store.last().await {
        Ok(last) => {
            if last.round == last_round {
                1
            } else {
                last.round
            }
        }
        _ => 1,
    };

    println!("Node Status: {}", last_round);

    println!("Starting from round {}", start_round);

    let request = SyncRequest {
        from_round: start_round,
        metadata: Some(Metadata {
            node_version: None,
            beacon_id: id.to_owned(),
            chain_hash: hex::decode(
                "ddb3665060932c267aacde99049ea31f3f5a049b1741c31cf71cd5d7d11a8da2",
            )
            .unwrap(),
        }),
    };

    let mut stream = client.sync_chain(request).await.unwrap().into_inner();

    let mut count = 0;

    println!();

    while let Some(item) = stream.next().await {
        let item = item.unwrap();
        let current_round = item.round;

        count += 1;
        if count % 300 == 0 {
            print!(
                "\r[INFO] received round {} {:.2}%",
                current_round,
                (current_round as f32 / last_round as f32) * 100.0
            );
            std::io::Write::flush(&mut std::io::stdout()).unwrap();
        }
        // println!("received:\n {item:?}");
        store.put(item.into()).await.unwrap();

        if current_round == last_round {
            println!("\nfinished!");
            break;
        }
    }
}
