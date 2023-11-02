SET 'auto.offset.reset' = 'earliest';

CREATE TABLE IF NOT EXISTS shard_biota_properties_d_i_d_table AS
    SELECT object_id,
           property_type,
           property_name,
           LATEST_BY_OFFSET(value) AS value,
           LATEST_BY_OFFSET(value_label) AS value_label,
           LATEST_BY_OFFSET(timestamp) AS timestamp
    FROM shard_biota_properties_d_i_d
    GROUP BY object_id, property_type, property_name
    EMIT CHANGES;

CREATE TABLE IF NOT EXISTS shard_biota_properties_int_table AS
    SELECT object_id,
           property_type,
           property_name,
           LATEST_BY_OFFSET(value) AS value,
           LATEST_BY_OFFSET(value_label) AS value_label,
           LATEST_BY_OFFSET(timestamp) AS timestamp
    FROM shard_biota_properties_int
    GROUP BY object_id, property_type, property_name
    EMIT CHANGES;

CREATE TABLE IF NOT EXISTS shard_biota_properties_int64_table AS
    SELECT object_id,
           property_type,
           property_name,
           LATEST_BY_OFFSET(value) AS value,
           LATEST_BY_OFFSET(timestamp) AS timestamp
    FROM shard_biota_properties_int64
    GROUP BY object_id, property_type, property_name
    EMIT CHANGES;

CREATE TABLE IF NOT EXISTS shard_biota_properties_string_table AS
    SELECT object_id,
           property_type,
           property_name,
           LATEST_BY_OFFSET(value) AS value,
           LATEST_BY_OFFSET(timestamp) AS timestamp
    FROM shard_biota_properties_string
    GROUP BY object_id, property_type, property_name
    EMIT CHANGES;
