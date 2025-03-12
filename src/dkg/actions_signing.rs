use prost_types::Timestamp;

/// Implementation for UTC 0, aligned to https://pkg.go.dev/time#Time.MarshalBinary [go 1.22.10]
#[allow(dead_code)]
fn enc_timestamp(t: Timestamp) -> [u8; 15] {
    // Add delta(year 1, unix epoch)
    let sec = t.seconds + 62135596800;
    let nsec = t.nanos;

    [
        1, // timeBinaryVersionV1
        (sec >> 56) as u8,
        (sec >> 48) as u8,
        (sec >> 40) as u8,
        (sec >> 32) as u8,
        (sec >> 24) as u8,
        (sec >> 16) as u8,
        (sec >> 8) as u8,
        (sec) as u8,
        (nsec >> 24) as u8,
        (nsec >> 16) as u8,
        (nsec >> 8) as u8,
        (nsec) as u8,
        // zone offset
        0xFF,
        0xFF,
    ]
}

#[test]
fn enc_timeout() {
    // Test data from runtime https://github.com/drand/drand/blob/v2.1.0/internal/dkg/actions_signing.go#L130
    struct Vector {
        plain: Timestamp,
        enc: &'static str,
    }

    let vectors = [
        Vector {
            plain: Timestamp {
                seconds: 1741744764,
                nanos: 594379669,
            },
            enc: "010000000edf62e17c236d8395ffff",
        },
        Vector {
            plain: Timestamp {
                seconds: 1741745624,
                nanos: 86180161,
            },
            enc: "010000000edf62e4d805230141ffff",
        },
        Vector {
            plain: Timestamp {
                seconds: 1741745738,
                nanos: 467268517,
            },
            enc: "010000000edf62e54a1bd9f3a5ffff",
        },
        Vector {
            plain: Timestamp {
                seconds: 1741745868,
                nanos: 483295650,
            },
            enc: "010000000edf62e5cc1cce81a2ffff",
        },
        Vector {
            plain: Timestamp {
                seconds: 1741745941,
                nanos: 752070485,
            },
            enc: "010000000edf62e6152cd3af55ffff",
        },
        Vector {
            plain: Timestamp {
                seconds: 1741746194,
                nanos: 656876372,
            },
            enc: "010000000edf62e71227272354ffff",
        },
    ];

    vectors
        .iter()
        .for_each(|v| assert!(enc_timestamp(v.plain) == *hex::decode(v.enc).unwrap()));
}
