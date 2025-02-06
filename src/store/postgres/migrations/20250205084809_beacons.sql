-- Add migration script here

-- Description: Create beacons table
CREATE TABLE beacons (
    id    SERIAL,
    name  TEXT    NOT NULL,

    PRIMARY KEY (ID)
);

-- Description: Create beacons table index
CREATE UNIQUE INDEX index_beacons_name ON beacons (name);

CREATE TABLE beacon_details (
    beacon_id     INT     NOT NULL,
    round         BIGINT  NOT NULL,
    signature     BYTEA   NOT NULL,

    CONSTRAINT pk_beacon_id_round PRIMARY KEY (beacon_id, round),
    CONSTRAINT fk_beacon_id FOREIGN KEY (beacon_id) REFERENCES beacons(id) ON DELETE CASCADE
);
