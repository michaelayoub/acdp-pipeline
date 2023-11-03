SET 'auto.offset.reset' = 'earliest';

CREATE TABLE shard_biota_properties_position_enriched AS
    SELECT object_id,
           property_name,
           LATEST_BY_OFFSET(obj_cell_id) AS obj_cell_id,
           LATEST_BY_OFFSET(origin_x) AS origin_x,
           LATEST_BY_OFFSET(origin_y) AS origin_y,
           LATEST_BY_OFFSET(origin_z) AS origin_z,
           ace_map_coords(LATEST_BY_OFFSET(obj_cell_id), LATEST_BY_OFFSET(origin_x), LATEST_BY_OFFSET(origin_y), LATEST_BY_OFFSET(origin_z)) as coordinates,
           ace_nearest_poi(LATEST_BY_OFFSET(obj_cell_id), LATEST_BY_OFFSET(origin_x), LATEST_BY_OFFSET(origin_y), LATEST_BY_OFFSET(origin_z), 5) as five_nearest_pois
    FROM shard_biota_properties_position
    GROUP BY object_id, property_name
    EMIT CHANGES;
