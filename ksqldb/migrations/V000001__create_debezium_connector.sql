CREATE SOURCE CONNECTOR IF NOT EXISTS ace_reader WITH (
    'connector.class' = 'io.debezium.connector.mysql.MySqlConnector',
    'database.hostname' = 'host.lima.internal',
    'database.port' = '3306',
    'database.user' = 'debezium',
    'database.password' = 'debezium',
    'database.server.id' = '1234',
    'database.server.name' = 'ace',
    'topic.prefix' = 'ace',
    'database.connectionTimeZone' = 'America/Chicago',
    'database.allowPublicKeyRetrieval' = 'true',
    'database.include.list' = 'ace_world,ace_auth,ace_shard',
    'database.history.kafka.bootstrap.servers' = 'broker:9092',
    'database.history.kafka.topic' = 'dbhistory',
    'include.schema.changes' = 'true',
    'schema.history.internal.kafka.topic' ='ace.schema',
    'schema.history.internal.kafka.bootstrap.servers' ='broker:9092',
    'transforms' = 'unwrap',
    'transforms.unwrap.type'='io.debezium.transforms.ExtractNewRecordState',
    'transforms.unwrap.drop.tombstones'='false',
    'transforms.unwrap.delete.handling.mode'='rewrite',
    'transforms.unwrap.add.fields'='source.ts_ms'
);
