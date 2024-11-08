USE DATABASE predictit_db;
USE SCHEMA bronze;

-- 1) get previous day's date and convert that into naming convention used in json file format
SET yesterday_date = TO_VARCHAR(DATEADD(day, -1, CURRENT_DATE()), 'MMDDYYYY');

SET filename_pattern = CONCAT('predictit_data_raw', $yesterday_date, '.json');

-- 2) copy data from that file using pattern matching for that day within file name

COPY INTO raw_data 
FROM @predictit_s3_stage
pattern = $filename_pattern
file_format = json_format;

-- add in column for date of each market in raw_data
CREATE OR REPLACE TABLE raw_data_with_date AS
SELECT 
    $1 as jsondata,
    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, '\\d{8}', 1, 1, 'e'), 'MMDDYYYY') AS market_date
FROM @predictit_s3_stage
(FILE_FORMAT => json_format);