{{ log('Current Database: ' ~ target.database, info=True) }}
{{ log('Current Schema: ' ~ target.schema, info=True) }}

WITH src_tags AS (
    SELECT * FROM {{ source('NETFLIX_DATA','r_movies') }}
)

SELECT
    *
FROM src_tags
LIMIT 10