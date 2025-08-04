{{ config(materialized = 'table') }}

WITH fct_ratings AS (
    SELECT * FROM {{ ref('fct_ratings') }}
),
seed_dates AS (
    SELECT * FROM {{ ref('seed_movie_release_dates') }}
),
movie_rating_agg AS (
    SELECT
        movie_id,
        AVG(rating) AS average_rating,
        COUNT(rating) AS num_ratings,
        COUNT(DISTINCT user_id) AS num_unique_raters,
        MAX(rating) AS max_rating,
        MIN(rating) AS min_rating,
        STDDEV_POP(rating) AS rating_stddev,
        MAX(rating_timestamp) AS last_rating_timestamp
    FROM {{ ref('fct_ratings')}}
    GROUP BY movie_id
),
movie_tags_agg AS (
    -- Aggregate tags data to count the number of unique tags for each movie.
    SELECT
        movie_id,
        COUNT(DISTINCT tag_name) AS num_tags
    FROM {{ref('dim_movies_with_tags')}}
    GROUP BY movie_id
),
movie_genome_agg AS (
    -- Aggregate genome scores to calculate the average relevance score
    -- for each movie across all its associated tags.
    SELECT
        movie_id,
        AVG(relevance_score) AS avg_genome_relevance
    FROM {{ref('dim_movies_with_tags')}}
    GROUP BY movie_id
)

SELECT 
    f.*,
    CASE
        WHEN d.release_date IS NULL THEN 'unknown'
        ELSE 'known'
    END AS release_info_available,
    m.movie_title,
    m.release_year,
    m.genre_array,
    m.num_distinct_genres,
    m.primary_genre,
    COALESCE(r.average_rating, 0.0) AS average_rating,
    COALESCE(r.num_ratings, 0) AS num_ratings,
    COALESCE(r.num_unique_raters, 0) AS num_unique_raters,
    COALESCE(r.max_rating, 0.0) AS max_rating,
    COALESCE(r.min_rating, 0.0) AS min_rating,
    COALESCE(r.rating_stddev, 0.0) AS rating_stddev,
    COALESCE(
        DATEDIFF(
            'day',
            TO_DATE(TO_TIMESTAMP(r.last_rating_timestamp)),
            CURRENT_DATE()
        ),
        NULL
    ) AS days_since_last_rating,
    COALESCE(t.num_tags, 0) AS num_tags,
    COALESCE(g.avg_genome_relevance, 0.0) AS avg_genome_relevance
FROM 
    fct_ratings f
LEFT JOIN 
    seed_dates d ON f.movie_id = d.movie_id
LEFT JOIN
    dim_movies m ON f.movie_id = m.movie_id
LEFT JOIN
    movie_rating_agg r ON f.movie_id = r.movie_id
LEFT JOIN
    movie_tags_agg t ON f.movie_id = t.movie_id
LEFT JOIN
    movie_genome_agg g ON f.movie_id = g.movie_id
LIMIT 100