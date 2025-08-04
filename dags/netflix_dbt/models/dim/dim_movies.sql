WITH src_movies AS (
    SELECT * FROM {{ ref('src_movies') }}
)
SELECT
    movie_id,
    INITCAP(TRIM(title)) AS movie_title,
    SPLIT(genres, '|') AS genre_array,
    genres,
    TRY_CAST(REGEXP_SUBSTR(movie_title, '\\((\\d{4})\\)', 1, 1, 'e', 1) AS INTEGER) AS release_year,
    ARRAY_SIZE(genre_array) AS num_distinct_genres,
    SPLIT_PART(genres, '|', 1) AS primary_genre,
FROM src_movies
