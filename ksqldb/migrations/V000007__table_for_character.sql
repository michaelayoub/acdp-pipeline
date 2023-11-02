SET 'auto.offset.reset' = 'earliest';

CREATE TABLE IF NOT EXISTS shard_character_table AS
    SELECT object_id,
           LATEST_BY_OFFSET(account_id),
           LATEST_BY_OFFSET(name) AS name,
           LATEST_BY_OFFSET(is_plussed) AS is_plussed,
           LATEST_BY_OFFSET(is_deleted) AS is_deleted,
           LATEST_BY_OFFSET(total_logins) AS total_logins,
           LATEST_BY_OFFSET(delete_timestamp) AS delete_timestamp,
           LATEST_BY_OFFSET(last_login_timestamp) AS last_used_timestamp,
           LATEST_BY_OFFSET(timestamp) AS timestamp
    FROM shard_character
    GROUP BY object_id
    EMIT CHANGES;
