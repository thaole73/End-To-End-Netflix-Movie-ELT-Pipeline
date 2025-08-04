-- Load raw_links
CREATE TABLE raw_links (
  movieId INTEGER,
  imdbId INTEGER,
  tmdbId INTEGER
);

-- COPY INTO raw_links
-- FROM '@netflixstage/links.csv'
-- FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
-- ON_ERROR = 'CONTINUE'
-- PURGE = TRUE;