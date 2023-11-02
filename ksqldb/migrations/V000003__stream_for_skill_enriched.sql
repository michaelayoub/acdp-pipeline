SET 'auto.offset.reset' = 'earliest';

ASSERT TOPIC 'SHARD_BIOTA_PROPERTIES_SKILL_ENRICHED' TIMEOUT 1 MINUTE;
CREATE SOURCE STREAM IF NOT EXISTS shard_biota_properties_skill_enriched_stream  WITH (kafka_topic = 'SHARD_BIOTA_PROPERTIES_SKILL_ENRICHED', format = 'avro');
