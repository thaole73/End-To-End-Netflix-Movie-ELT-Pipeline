-- Load raw_movies
CREATE TABLE raw_movies (
  movieId INTEGER,
  title STRING,
  genres STRING
);

-- COPY INTO raw_movies
-- FROM '@netflixstage/movies.csv'
-- FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
-- ON_ERROR = 'CONTINUE'
-- PURGE = TRUE;