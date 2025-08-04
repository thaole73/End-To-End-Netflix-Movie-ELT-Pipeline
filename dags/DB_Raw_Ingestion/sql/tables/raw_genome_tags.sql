-- Load raw_genome_tags
CREATE TABLE raw_genome_tags (
  tagId INTEGER,
  tag STRING
);

-- COPY INTO raw_genome_tags
-- FROM '@netflixstage/genome-tags.csv'
-- FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
-- ON_ERROR = 'CONTINUE'
-- PURGE = TRUE;