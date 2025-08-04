-- Load raw_ratings
CREATE TABLE raw_ratings (
  userId INTEGER,
  movieId INTEGER,
  rating FLOAT,
  timestamp BIGINT
);

-- COPY INTO raw_ratings
-- FROM '@netflixstage/ratings.csv'
-- FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
-- ON_ERROR = 'CONTINUE'
-- PURGE = TRUE;