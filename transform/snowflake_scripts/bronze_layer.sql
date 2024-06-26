create or replace stage my_s3_stage
 storage_integration = s3_int
 url = 's3://predictit-de-project/raw/'
 ;

 list @my_s3_stage;

 -- load raw data into first landing table using single variant column

 create or replace table raw_data (
     jsondata variant not null
);

CREATE FILE FORMAT json_format
  TYPE = 'JSON';
  
copy into raw_data 
from @my_s3_stage
file_format = json_format;


-- Make sure data got loaded in correctly
SELECT *
FROM raw_data;


-- Normalization
-- First table: market dimension table )
CREATE TABLE market_dim_bronze AS (
    
    SELECT
        unnested.value:ID::NUMERIC AS market_id,
        unnested.value:Name AS long_name,
        unnested.value:ShortName AS short_name,
        unnested.value:URL as url,
        unnested.value:Image as image
    FROM raw_data,
        LATERAL FLATTEN(jsondata) AS markets,
        LATERAL FLATTEN(markets.value:Markets) AS marketdata,
        LATERAL FLATTEN(marketdata.value) as unnested
    )
;


-- Contract table
CREATE TABLE contract_fact_bronze AS (
    SELECT
    DISTINCT unnested.value:ID::NUMERIC AS market_id,
    CASE 
        -- deal with markets that have more than one contract in array form
        WHEN ARRAY_SIZE(unnested.value:Contracts:MarketContract) > 1 THEN
            (SELECT contract_data.value:ID::STRING)
        ELSE unnested.value:Contracts:MarketContract:ID::STRING
    END AS contract_id,
    CASE 
        WHEN ARRAY_SIZE(unnested.value:Contracts:MarketContract) > 1 THEN
            (SELECT contract_data.value:DateEnd::STRING)
        ELSE unnested.value:Contracts:MarketContract:DateEnd::STRING
    END AS end_date,
    CASE 
        WHEN ARRAY_SIZE(unnested.value:Contracts:MarketContract) > 1 THEN
            (SELECT contract_data.value:Image::STRING)
        ELSE unnested.value:Contracts:MarketContract:Image::STRING
    END AS image,
    CASE 
        WHEN ARRAY_SIZE(unnested.value:Contracts:MarketContract) > 1 THEN
            (SELECT contract_data.value:Name::STRING)
        ELSE unnested.value:Contracts:MarketContract:Name::STRING
    END AS name,
    CASE 
        WHEN ARRAY_SIZE(unnested.value:Contracts:MarketContract) > 1 THEN
            (SELECT contract_data.value:ShortName::STRING)
        ELSE unnested.value:Contracts:MarketContract:ShortName::STRING
    END AS short_name,
    CASE 
        WHEN ARRAY_SIZE(unnested.value:Contracts:MarketContract) > 1 THEN
            (SELECT contract_data.value:Status::STRING)
        ELSE unnested.value:Contracts:MarketContract:Status::STRING
    END AS status,
    CASE 
        WHEN ARRAY_SIZE(unnested.value:Contracts:MarketContract) > 1 THEN
            (SELECT contract_data.value:LastTradePrice::DECIMAL(10,2))
        ELSE unnested.value:Contracts:MarketContract:LastTradePrice::DECIMAL(10,2)
    END AS last_trade_price,
    CASE 
        WHEN ARRAY_SIZE(unnested.value:Contracts:MarketContract) > 1 THEN
            (SELECT contract_data.value:BestBuyYesCost::DECIMAL(10,2))
        ELSE unnested.value:Contracts:MarketContract:BestBuyYesCost::DECIMAL(10,2)
    END AS Best_Buy_Yes_Cost,
    CASE
        -- deal with markets that have more than one contract in array form
        WHEN ARRAY_SIZE(unnested.value:Contracts:MarketContract) > 1 THEN
            (SELECT contract_data.value:BestBuyNoCost)
                    --contract_data.value:DateEnd AS end_date)
        ELSE unnested.value:Contracts:MarketContract:BestBuyNoCost::DECIMAL(10,2)
    END AS Best_Buy_No_Cost,
    CASE 
        -- deal with markets that have more than one contract in array form
        WHEN ARRAY_SIZE(unnested.value:Contracts:MarketContract) > 1 THEN
            (SELECT contract_data.value:BestSellYesCost)
                    --contract_data.value:DateEnd AS end_date)
        ELSE unnested.value:Contracts:MarketContract:BestSellYesCost::DECIMAL(10,2)
    END AS Best_Sell_Yes_Cost,
    CASE 
        -- deal with markets that have more than one contract in array form
        WHEN ARRAY_SIZE(unnested.value:Contracts:MarketContract) > 1 THEN
            (SELECT contract_data.value:BestSellNoCost::DECIMAL(10,2))
                    --contract_data.value:DateEnd AS end_date)
        ELSE unnested.value:Contracts:MarketContract:BestSellNoCost::DECIMAL(10,2)
    END AS Best_sell_no_Cost,
    CASE 
        -- deal with markets that have more than one contract in array form
        WHEN ARRAY_SIZE(unnested.value:Contracts:MarketContract) > 1 THEN
            (SELECT contract_data.value:LastClosePrice::DECIMAL(10,2))
                    --contract_data.value:DateEnd AS end_date)
        ELSE unnested.value:Contracts:MarketContract:LastClosePrice::DECIMAL(10,2)
    END AS Last_Close_Price
    FROM
    raw_data,
    LATERAL FLATTEN(jsondata) AS markets,
    LATERAL FLATTEN(markets.value:Markets) AS marketdata,
    LATERAL FLATTEN(marketdata.value) AS unnested,
    LATERAL FLATTEN(PARSE_JSON(unnested.value:Contracts:MarketContract)) AS contract_data
);


    
-- Verify results of CTAS statements