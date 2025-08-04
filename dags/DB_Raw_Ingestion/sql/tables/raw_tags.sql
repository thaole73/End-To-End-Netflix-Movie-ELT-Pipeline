-- Load raw_tags
CREATE TABLE raw_tags (
  userId INTEGER,
  movieId INTEGER,
  tag STRING,
  timestamp BIGINT
);

-- COPY INTO raw_tags
-- FROM '@netflixstage/tags.csv'
-- FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
-- ON_ERROR = 'CONTINUE'
-- PURGE = TRUE;