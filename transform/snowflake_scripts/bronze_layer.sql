 -- create stage
 CREATE OR REPLACE STAGE IF NOT EXISTS predictit_s3_stage
    URL = 's3://predictit-de-project/raw/'
    STORAGE_INTEGRATION = s3_int;

 -- load raw data into first landing table using single variant column

CREATE OR REPLACE TABLE raw_data (
     jsondata variant NOT NULL
);

CREATE FILE FORMAT json_format
  TYPE = 'JSON';


COPY INTO raw_data 
FROM @predictit_s3_stage
file_format = json_format;

-- add in column for date of each market in raw_data
CREATE OR REPLACE TABLE raw_data_with_date AS
SELECT 
    $1 as jsondata,
    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, '\\d{8}', 1, 1, 'e'), 'MMDDYYYY') AS market_date
FROM @my_s3_stage
(FILE_FORMAT => json_format);