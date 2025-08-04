WITH src_tags AS (
    SELECT * FROM {{ ref('snap_tags') }}
)

SELECT
    row_key,
    user_id,
    movie_id,
    tag,
    tag_timestamp
FROM src_tags
WHERE dbt_valid_to IS NULL