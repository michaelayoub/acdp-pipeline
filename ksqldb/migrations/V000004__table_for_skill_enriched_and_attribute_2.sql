SET 'auto.offset.reset' = 'earliest';

CREATE TABLE IF NOT EXISTS shard_biota_properties_skill_enriched_collected AS
    SELECT ROWKEY->object_id AS object_id,
           ace_skill_collect(s.enriched_skill_details) AS enriched_skill_details_list
    FROM shard_biota_properties_skill_enriched_stream s
    GROUP BY ROWKEY->object_id
    EMIT CHANGES;

CREATE TABLE IF NOT EXISTS shard_biota_properties_attribute_2nd_enriched AS
    SELECT a2.object_id AS object_id,
           a2.property_type AS property_type,
           a2.property_name AS property_name,
           a2.initial_level AS initial_level,
           a2.num_times_increased AS num_times_increased,
           a2.xp_spent AS xp_spent,
           CASE WHEN a2.property_name = 'MaxHealth'
                THEN CAST(ROUND(FILTER(c.attribute_details_list, x => x->property_name = 'ENDURANCE')[1]->current_level / 2.0 + a2.initial_level + a2.num_times_increased) AS BIGINT)
                WHEN a2.property_name = 'MaxStamina'
                THEN CAST(ROUND(FILTER(c.attribute_details_list, x => x->property_name = 'ENDURANCE')[1]->current_level / 1.0 + a2.initial_level + a2.num_times_increased) AS BIGINT)
                WHEN a2.property_name = 'MaxMana'
                THEN CAST(ROUND(FILTER(c.attribute_details_list, x => x->property_name = 'SELF')[1]->current_level + a2.initial_level / 1.0 + a2.num_times_increased) AS BIGINT)
                END AS current_level,
           a2.current_level_with_enchantments AS current_level_with_enchantments,
           a2.timestamp AS timestamp
    FROM shard_biota_properties_attribute_2nd_table a2
    JOIN shard_biota_properties_attribute_collected c ON a2.object_id = c.object_id
    EMIT CHANGES;
