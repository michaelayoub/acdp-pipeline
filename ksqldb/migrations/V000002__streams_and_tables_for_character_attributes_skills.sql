SET 'auto.offset.reset' = 'earliest';

ASSERT TOPIC 'ace.ace_shard.character' TIMEOUT 1 MINUTE;
CREATE STREAM IF NOT EXISTS shard_character_stream WITH (kafka_topic = 'ace.ace_shard.character', format = 'avro', timestamp='__source_ts_ms');

ASSERT TOPIC 'ace.ace_shard.biota_properties_attribute' TIMEOUT 1 MINUTE;
CREATE STREAM IF NOT EXISTS shard_biota_properties_attribute_stream WITH (kafka_topic = 'ace.ace_shard.biota_properties_attribute', format = 'avro', timestamp='__source_ts_ms');

ASSERT TOPIC 'ace.ace_shard.biota_properties_attribute_2nd' TIMEOUT 1 MINUTE;
CREATE STREAM IF NOT EXISTS shard_biota_properties_attribute_2nd_stream WITH (kafka_topic = 'ace.ace_shard.biota_properties_attribute_2nd', format = 'avro', timestamp='__source_ts_ms');

ASSERT TOPIC 'ace.ace_shard.biota_properties_skill' TIMEOUT 1 MINUTE;
CREATE STREAM IF NOT EXISTS shard_biota_properties_skill_stream WITH (kafka_topic = 'ace.ace_shard.biota_properties_skill', format = 'avro', timestamp='__source_ts_ms');

CREATE STREAM IF NOT EXISTS shard_character_rekeyed WITH (VALUE_FORMAT='AVRO') AS
    SELECT *
    FROM shard_character_stream
    PARTITION BY ROWKEY->ID;

CREATE STREAM IF NOT EXISTS shard_biota_properties_attribute_rekeyed WITH (VALUE_FORMAT='AVRO') AS
    SELECT *
    FROM shard_biota_properties_attribute_stream
    PARTITION BY ROWKEY->OBJECT_ID;

CREATE STREAM IF NOT EXISTS shard_biota_properties_attribute_2nd_rekeyed WITH (VALUE_FORMAT='AVRO') AS
    SELECT *
    FROM shard_biota_properties_attribute_2nd_stream
    PARTITION BY ROWKEY->OBJECT_ID;

CREATE STREAM IF NOT EXISTS shard_biota_properties_skill_rekeyed WITH (VALUE_FORMAT='AVRO') AS
    SELECT *
    FROM shard_biota_properties_skill_stream
    PARTITION BY ROWKEY->OBJECT_ID;

CREATE STREAM IF NOT EXISTS shard_character AS
    SELECT id_1 AS object_id,
           account_id,
           name,
           is_plussed,
           is_deleted,
           total_logins,
           NULLIF(FORMAT_TIMESTAMP(FROM_UNIXTIME(CAST(delete_time*1000 AS BIGINT)), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC'), FORMAT_TIMESTAMP(FROM_UNIXTIME(0), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC')) AS delete_timestamp,
           NULLIF(FORMAT_TIMESTAMP(FROM_UNIXTIME(CAST(last_login_timestamp*1000 AS BIGINT)), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC'), FORMAT_TIMESTAMP(FROM_UNIXTIME(0), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC')) AS last_login_timestamp,
           FORMAT_TIMESTAMP(FROM_UNIXTIME(__source_ts_ms), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC') AS timestamp
    FROM shard_character_rekeyed
    EMIT CHANGES;

CREATE STREAM IF NOT EXISTS shard_biota_properties_attribute AS
    SELECT object_id_1 AS object_id,
           type AS property_type,
           ace_enum('PropertyAttribute', type) AS property_name,
           init_Level AS initial_level,
           level_From_C_P AS num_times_increased,
           c_P_Spent AS xp_spent,
           init_Level + level_from_C_P AS current_level,
           FORMAT_TIMESTAMP(FROM_UNIXTIME(__source_ts_ms), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC') AS timestamp
    FROM shard_biota_properties_attribute_rekeyed
    EMIT CHANGES;

CREATE STREAM IF NOT EXISTS shard_biota_properties_attribute_2nd AS
    SELECT object_id_1 AS object_id,
           type AS property_type,
           ace_enum('PropertyAttribute2nd', type) AS property_name,
           init_level AS initial_level,
           level_from_C_P AS num_times_increased,
           c_P_Spent AS xp_spent,
           current_Level AS current_level_with_enchantments,
           FORMAT_TIMESTAMP(FROM_UNIXTIME(__source_ts_ms), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC') AS timestamp
    FROM shard_biota_properties_attribute_2nd_rekeyed;

CREATE STREAM IF NOT EXISTS shard_biota_properties_skill AS
    SELECT object_id_1 AS object_id,
           type AS property_type,
           ace_enum('Skill', type) AS property_name,
           init_Level AS initial_level,
           level_From_P_P AS num_times_increased,
           p_p AS xp_spent,
           s_a_c AS skill_advancement_class_type,
           ace_enum('SkillAdvancementClass', s_a_c) AS skill_advancement_class_name,
           NULLIF(FORMAT_TIMESTAMP(FROM_UNIXTIME(CAST(last_Used_Time*1000 AS BIGINT)), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC'), FORMAT_TIMESTAMP(FROM_UNIXTIME(0), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC')) AS last_used_timestamp,
           FORMAT_TIMESTAMP(FROM_UNIXTIME(__source_ts_ms), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC') AS timestamp
    FROM shard_biota_properties_skill_rekeyed;

CREATE STREAM IF NOT EXISTS shard_biota_properties_attribute_struct AS
    SELECT object_id,
           initial_level,
           num_times_increased,
           xp_spent,
           STRUCT(attribute_type := property_type,
                  attribute_name := property_name,
                  attribute_value := current_level) AS attribute_struct
    FROM shard_biota_properties_attribute
    EMIT CHANGES;

CREATE TABLE IF NOT EXISTS shard_biota_properties_attribute_collected AS
    SELECT object_id,
           ace_attribute_collect(attribute_struct) AS attribute_list
    FROM shard_biota_properties_attribute_struct
    GROUP BY object_id
    EMIT CHANGES;