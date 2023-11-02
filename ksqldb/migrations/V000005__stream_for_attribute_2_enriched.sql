SET 'auto.offset.reset' = 'earliest';

ASSERT TOPIC 'SHARD_BIOTA_PROPERTIES_ATTRIBUTE_2ND_ENRICHED' TIMEOUT 1 MINUTE;
CREATE SOURCE STREAM IF NOT EXISTS shard_biota_properties_attribute_2nd_enriched_stream  WITH (kafka_topic = 'SHARD_BIOTA_PROPERTIES_ATTRIBUTE_2ND_ENRICHED', format = 'avro');
