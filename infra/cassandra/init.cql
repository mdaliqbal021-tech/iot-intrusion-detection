CREATE KEYSPACE IF NOT EXISTS iot_replay
WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

CREATE TABLE IF NOT EXISTS iot_replay.predictions (
    run_id text,
    replay_id int,
    attack_score double,
    predicted_class int,
    PRIMARY KEY (run_id, replay_id)
);
