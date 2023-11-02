SET 'auto.offset.reset' = 'earliest';

CREATE TABLE shard_character_enriched AS
       SELECT sc.object_id,
              ac.attribute_details_list,
              sec.enriched_skill_details_list,
              a2c.enriched_attribute_2nd_details_list
       FROM shard_character_table sc
       JOIN shard_biota_properties_attribute_collected ac ON sc.object_id = ac.object_id
       JOIN shard_biota_properties_skill_enriched_collected sec ON sc.object_id = sec.object_id
       JOIN shard_biota_properties_attribute_2nd_enriched_collected a2c ON sc.object_id = a2c.object_Id
       EMIT CHANGES;
