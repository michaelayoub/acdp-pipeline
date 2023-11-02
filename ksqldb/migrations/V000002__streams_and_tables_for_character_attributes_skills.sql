SET 'auto.offset.reset' = 'earliest';

-- Source Streams

ASSERT TOPIC 'ace.ace_shard.character' TIMEOUT 1 MINUTE;
CREATE SOURCE STREAM IF NOT EXISTS shard_character_stream WITH (kafka_topic = 'ace.ace_shard.character', format = 'avro', timestamp='__source_ts_ms');

ASSERT TOPIC 'ace.ace_shard.biota_properties_attribute' TIMEOUT 1 MINUTE;
CREATE SOURCE STREAM IF NOT EXISTS shard_biota_properties_attribute_stream WITH (kafka_topic = 'ace.ace_shard.biota_properties_attribute', format = 'avro', timestamp='__source_ts_ms');

ASSERT TOPIC 'ace.ace_shard.biota_properties_attribute_2nd' TIMEOUT 1 MINUTE;
CREATE SOURCE STREAM IF NOT EXISTS shard_biota_properties_attribute_2nd_stream WITH (kafka_topic = 'ace.ace_shard.biota_properties_attribute_2nd', format = 'avro', timestamp='__source_ts_ms');

ASSERT TOPIC 'ace.ace_shard.biota_properties_skill' TIMEOUT 1 MINUTE;
CREATE SOURCE STREAM IF NOT EXISTS shard_biota_properties_skill_stream WITH (kafka_topic = 'ace.ace_shard.biota_properties_skill', format = 'avro', timestamp='__source_ts_ms');

ASSERT TOPIC 'ace.ace_shard.biota_properties_int' TIMEOUT 1 MINUTE;
CREATE SOURCE STREAM IF NOT EXISTS shard_biota_properties_int_stream WITH (kafka_topic = 'ace.ace_shard.biota_properties_int', format = 'avro', timestamp='__source_ts_ms' );

ASSERT TOPIC 'ace.ace_shard.biota_properties_int64' TIMEOUT 1 MINUTE;
CREATE SOURCE STREAM IF NOT EXISTS shard_biota_properties_int64_stream WITH (kafka_topic = 'ace.ace_shard.biota_properties_int64', format = 'avro', timestamp='__source_ts_ms' );

ASSERT TOPIC 'ace.ace_shard.biota_properties_string' TIMEOUT 1 MINUTE;
CREATE SOURCE STREAM IF NOT EXISTS shard_biota_properties_string_stream WITH (kafka_topic = 'ace.ace_shard.biota_properties_string', format = 'avro', timestamp='__source_ts_ms' );

ASSERT TOPIC 'ace.ace_shard.biota_properties_bool' TIMEOUT 1 MINUTE;
CREATE SOURCE STREAM IF NOT EXISTS shard_biota_properties_bool_stream WITH (kafka_topic = 'ace.ace_shard.biota_properties_bool', format = 'avro', timestamp='__source_ts_ms' );

ASSERT TOPIC 'ace.ace_shard.biota_properties_position' TIMEOUT 1 MINUTE;
CREATE SOURCE STREAM IF NOT EXISTS shard_biota_properties_position_stream WITH (kafka_topic = 'ace.ace_shard.biota_properties_position', format = 'avro', timestamp='__source_ts_ms' );

ASSERT TOPIC 'ace.ace_shard.biota_properties_float' TIMEOUT 1 MINUTE;
CREATE SOURCE STREAM IF NOT EXISTS shard_biota_properties_float_stream  WITH (kafka_topic = 'ace.ace_shard.biota_properties_float', format = 'avro', timestamp='__source_ts_ms' );

ASSERT TOPIC 'ace.ace_world.weenie_properties_d_i_d' TIMEOUT 1 MINUTE;
CREATE SOURCE STREAM IF NOT EXISTS shard_biota_properties_d_i_d_stream  WITH (kafka_topic = 'ace.ace_shard.biota_properties_d_i_d', format = 'avro', timestamp='__source_ts_ms' );

-- Streams

CREATE STREAM IF NOT EXISTS shard_character AS
    SELECT ROWKEY->id AS object_id,
           account_id,
           name,
           is_plussed,
           is_deleted,
           total_logins,
           NULLIF(FORMAT_TIMESTAMP(FROM_UNIXTIME(CAST(delete_time*1000 AS BIGINT)), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC'), FORMAT_TIMESTAMP(FROM_UNIXTIME(0), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC')) AS delete_timestamp,
           NULLIF(FORMAT_TIMESTAMP(FROM_UNIXTIME(CAST(last_login_timestamp*1000 AS BIGINT)), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC'), FORMAT_TIMESTAMP(FROM_UNIXTIME(0), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC')) AS last_login_timestamp,
           FORMAT_TIMESTAMP(FROM_UNIXTIME(ROWTIME), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC') AS timestamp
    FROM shard_character_stream
    PARTITION BY ROWKEY->id
    EMIT CHANGES;

CREATE STREAM IF NOT EXISTS shard_biota_properties_attribute AS
    SELECT ROWKEY->object_id AS object_id,
           type AS property_type,
           ace_enum('PropertyAttribute', type) AS property_name,
           init_level AS initial_level,
           level_from_c_p AS num_times_increased,
           c_p_spent AS xp_spent,
           init_level + level_from_c_p AS current_Level,
           FORMAT_TIMESTAMP(FROM_UNIXTIME(ROWTIME), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC') AS timestamp
    FROM shard_biota_properties_attribute_stream
    PARTITION BY ROWKEY->object_id
    EMIT CHANGES;

CREATE STREAM IF NOT EXISTS shard_biota_properties_attribute_2nd AS
    SELECT ROWKEY->object_id AS object_id,
           type AS property_type,
           ace_enum('PropertyAttribute2nd', type) AS property_name,
           init_level AS initial_level,
           level_from_c_p AS num_times_increased,
           c_p_spent AS xp_spent,
           current_level AS current_level_with_enchantments,
           FORMAT_TIMESTAMP(FROM_UNIXTIME(ROWTIME), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC') AS timestamp
    FROM shard_biota_properties_attribute_2nd_stream
    PARTITION BY ROWKEY->object_id
    EMIT CHANGES;

CREATE STREAM IF NOT EXISTS shard_biota_properties_skill AS
    SELECT ROWKEY->object_id AS object_id,
           type AS property_type,
           ace_enum('Skill', type) AS property_name,
           init_level AS initial_level,
           level_from_p_p AS num_times_increased,
           p_p AS xp_spent,
           s_a_c AS skill_advancement_class_type,
           ace_enum('SkillAdvancementClass', s_a_c) AS skill_advancement_class_name,
           NULLIF(FORMAT_TIMESTAMP(FROM_UNIXTIME(CAST(last_Used_Time*1000 AS BIGINT)), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC'), FORMAT_TIMESTAMP(FROM_UNIXTIME(0), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC')) AS last_used_timestamp,
           FORMAT_TIMESTAMP(FROM_UNIXTIME(ROWTIME), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC') AS timestamp
    FROM shard_biota_properties_skill_stream
    PARTITION BY ROWKEY->object_id
    EMIT CHANGES;

CREATE STREAM IF NOT EXISTS shard_biota_properties_int AS
    SELECT ROWKEY->object_Id AS object_id,
           type AS property_type,
           ace_enum('PropertyInt', type) AS property_name,
           value,
           ace_enum('PropertyInt', type, value) AS value_label,
           FORMAT_TIMESTAMP(FROM_UNIXTIME(ROWTIME), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC') AS timestamp
    FROM shard_biota_properties_int_stream
    PARTITION BY ROWKEY->object_id
    EMIT CHANGES;

CREATE STREAM IF NOT EXISTS shard_biota_properties_int64 AS
    SELECT ROWKEY->object_Id AS object_id,
           type AS property_type,
           ace_enum('PropertyInt64', type) AS property_name,
           value,
           FORMAT_TIMESTAMP(FROM_UNIXTIME(ROWTIME), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC') AS timestamp
    FROM shard_biota_properties_int64_stream
    PARTITION BY ROWKEY->object_id
    EMIT CHANGES;

CREATE STREAM IF NOT EXISTS shard_biota_properties_string AS
    SELECT ROWKEY->object_Id AS object_id,
           type AS property_type,
           ace_enum('PropertyString', type) AS property_name,
           value,
           FORMAT_TIMESTAMP(FROM_UNIXTIME(ROWTIME), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC') AS timestamp
    FROM shard_biota_properties_string_stream
    PARTITION BY ROWKEY->object_id
    EMIT CHANGES;

CREATE STREAM IF NOT EXISTS shard_biota_properties_bool AS
    SELECT ROWKEY->object_Id AS object_id,
           type AS property_type,
           ace_enum('PropertyBool', type) AS property_name,
           value,
           FORMAT_TIMESTAMP(FROM_UNIXTIME(ROWTIME), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC') AS timestamp
    FROM shard_biota_properties_bool_stream
    PARTITION BY ROWKEY->object_id
    EMIT CHANGES;

CREATE STREAM IF NOT EXISTS shard_biota_properties_position AS
    SELECT ROWKEY->object_id AS object_id,
           position_Type AS property_type,
           ace_enum('PositionType', position_Type) AS property_name,
            obj_Cell_Id AS obj_cell_id,
           origin_X AS origin_x,
           origin_Y AS origin_y,
           origin_Z AS origin_z,
           angles_W AS angles_w,
           angles_X AS angles_x,
           angles_Y AS angles_y,
           angles_Z AS angles_z,
           FORMAT_TIMESTAMP(FROM_UNIXTIME(ROWTIME), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC') AS timestamp
    FROM shard_biota_properties_position_stream
    PARTITION BY ROWKEY->object_id
    EMIT CHANGES;

CREATE STREAM IF NOT EXISTS shard_biota_properties_float AS
    SELECT ROWKEY->object_Id AS object_id,
           type AS property_type,
           ace_enum('PropertyFloat', type) AS property_name,
           value,
           FORMAT_TIMESTAMP(FROM_UNIXTIME(ROWTIME), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC') AS timestamp
    FROM shard_biota_properties_float_stream
    PARTITION BY ROWKEY->object_id
    EMIT CHANGES;

CREATE STREAM IF NOT EXISTS shard_biota_properties_d_i_d AS
    SELECT ROWKEY->object_id AS object_id,
           type AS property_type,
           ace_enum('PropertyDataId', type) AS property_name,
           value,
           ace_enum('PropertyDataId', type, value) AS value_label,
           FORMAT_TIMESTAMP(FROM_UNIXTIME(ROWTIME), 'yyyy-MM-dd''T''HH:mm:ssX', 'UTC') AS timestamp
    FROM shard_biota_properties_d_i_d_stream
    PARTITION BY ROWKEY->object_id
    EMIT CHANGES;

-- Derived

CREATE TABLE IF NOT EXISTS shard_biota_properties_skill_structured AS
    SELECT object_id,
           property_type,
           struct(property_type := LATEST_BY_OFFSET(property_type),
                  property_name := LATEST_BY_OFFSET(property_name),
                  initial_level := LATEST_BY_OFFSET(initial_level),
                  num_times_increased := LATEST_BY_OFFSET(num_times_increased),
                  xp_spent := LATEST_BY_OFFSET(xp_spent),
                  skill_advancement_class_type := LATEST_BY_OFFSET(skill_advancement_class_type),
                  skill_advancement_class_name := LATEST_BY_OFFSET(skill_advancement_class_name),
                  last_used_timestamp := LATEST_BY_OFFSET(last_used_timestamp)) as skill_details
    FROM shard_biota_properties_skill
    GROUP BY object_id, property_type
    EMIT CHANGES;

CREATE TABLE IF NOT EXISTS shard_biota_properties_attribute_2nd_table AS
    SELECT object_id,
           property_type,
           property_name,
           LATEST_BY_OFFSET(initial_level) AS initial_level,
           LATEST_BY_OFFSET(num_times_increased) AS num_times_increased,
           LATEST_BY_OFFSET(xp_spent) AS xp_spent,
           LATEST_BY_OFFSET(current_level_with_enchantments) AS current_level_with_enchantments,
           LATEST_BY_OFFSET(timestamp) AS timestamp
    FROM shard_biota_properties_attribute_2nd
    GROUP BY object_id, property_type, property_name
    EMIT CHANGES;

CREATE TABLE IF NOT EXISTS shard_biota_properties_attribute_collected AS
    SELECT object_id,
           ace_attribute_collect(
                struct(property_type := property_type,
                       property_name := property_name,
                       initial_level := initial_level,
                       num_times_increased := num_times_increased,
                       xp_spent := xp_spent,
                       current_level := current_level)) AS attribute_details_list
    FROM shard_biota_properties_attribute
    GROUP BY object_id
    EMIT CHANGES;

CREATE TABLE IF NOT EXISTS shard_biota_properties_skill_enriched AS
    SELECT s.object_id AS object_id,
           s.property_type AS property_type,
           ace_skill(s.skill_details, c.attribute_details_list) AS enriched_skill_details
    FROM shard_biota_properties_skill_structured s
    JOIN shard_biota_properties_attribute_collected c on s.object_id = c.object_id
    EMIT CHANGES;
