CREATE KEYSPACE IF NOT EXISTS iot_replay
WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

CREATE TABLE IF NOT EXISTS iot_replay.predictions (
    run_id text,
    replay_id int,
    attack_score double,
    predicted_class int,
    PRIMARY KEY (run_id, replay_id)
);

CREATE TABLE IF NOT EXISTS iot_replay.completed_runs (
    run_id text PRIMARY KEY,
    completed_at timestamp,
    prediction_count int
);

ALTER TABLE iot_replay.completed_runs ADD IF NOT EXISTS display_number int;

CREATE TABLE IF NOT EXISTS iot_replay.run_number_sequence (
    sequence_name text PRIMARY KEY,
    last_number int
);

INSERT INTO iot_replay.run_number_sequence (sequence_name, last_number)
VALUES ('completed_runs', 0) IF NOT EXISTS;
