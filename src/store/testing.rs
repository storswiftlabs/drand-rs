use crate::store::Beacon;

use super::{BeaconCursor, Store};

pub async fn test_store<T: Store>(store: &T) {
    store
        .put(Beacon {
            previous_sig: vec![1, 2, 3],
            round: 0,
            signature: vec![4, 5, 6],
        })
        .await
        .expect("Failed to put beacon");

    store
        .put(Beacon {
            previous_sig: vec![4, 5, 6],
            round: 1,
            signature: vec![7, 8, 9],
        })
        .await
        .expect("Failed to put beacon");

    let b = store.get(1).await.expect("Failed to get beacon");

    assert_eq!(
        b.previous_sig,
        vec![4, 5, 6],
        "Previous signature does not match"
    );
    assert_eq!(b.round, 1, "Round does not match");
    assert_eq!(b.signature, vec![7, 8, 9], "Signature does not match");

    let len = store.len().await.expect("Failed to get length");

    assert_eq!(len, 2, "Length should be 2");

    let b = store.last().await.expect("Failed to get last");
    assert_eq!(
        b.previous_sig,
        vec![4, 5, 6],
        "Previous signature does not match"
    );
    assert_eq!(b.round, 1, "Round does not match");
    assert_eq!(b.signature, vec![7, 8, 9], "Signature does not match");

    store.del(1).await.expect("Failed to delete");
    let len = store.len().await.expect("Failed to get length");
    assert_eq!(len, 1, "Length should be 1");

    store.del(0).await.expect("Failed to delete");
    let len = store.len().await.expect("Failed to get length");
    assert_eq!(len, 0, "Length should be 0");
}

async fn prepare_cursor_data<T: Store>(store: &T) {
    for i in 5..35 {
        if i % 3 == 0 {
            continue;
        }

        store
            .put(Beacon {
                previous_sig: vec![i, i + 1],
                round: i as u64,
                signature: vec![i + 1, i + 2],
            })
            .await
            .expect("Failed to put");
    }
}

pub async fn test_cursor<T: Store>(store: &T) {
    prepare_cursor_data(store).await;

    let len = store.len().await.unwrap();

    println!("{}", len);

    let mut cursor = store.cursor();

    let first = cursor
        .first()
        .await
        .expect("Failed to get first")
        .expect("Failed to get first, empty");

    assert_eq!(first.round, 5);

    let next = cursor
        .next()
        .await
        .expect("Failed to get next")
        .expect("Failed to get next, empty");
    assert_eq!(next.round, 7);

    let next = cursor
        .next()
        .await
        .expect("Failed to get next")
        .expect("Failed to get next, empty");
    assert_eq!(next.round, 8);

    let last = cursor
        .last()
        .await
        .expect("Failed to get last")
        .expect("Failed to get last, empty");

    assert_eq!(last.round, 34);

    let seek = cursor
        .seek(20)
        .await
        .expect("Failed to seek")
        .expect("Failed to seek, empty");

    assert_eq!(seek.round, 20);
}
