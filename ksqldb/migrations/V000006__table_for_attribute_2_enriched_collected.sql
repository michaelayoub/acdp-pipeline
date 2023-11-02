SET 'auto.offset.reset' = 'earliest';

CREATE TABLE IF NOT EXISTS shard_biota_properties_attribute_2nd_enriched_collected AS
    SELECT ROWKEY->object_id AS object_id,
           ace_attribute2_collect(struct(
              property_type := ROWKEY->property_type,
              property_name := ROWKEY->property_name,
              initial_level := initial_level,
              num_times_increased := num_times_increased,
              xp_spent := xp_spent,
              current_level := current_level,
              current_level_with_enchantments := current_level_with_enchantments)) AS enriched_attribute_2nd_details_list
    FROM shard_biota_properties_attribute_2nd_enriched_stream
    GROUP BY ROWKEY->object_id
    EMIT CHANGES;
