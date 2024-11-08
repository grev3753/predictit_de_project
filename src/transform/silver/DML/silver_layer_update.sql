USE DATABASE predictit_db;
USE SCHEMA silver;

-- update silver tables

MERGE INTO market_dim_silver AS target
    USING     
        ( SELECT DISTINCT
            unnested.value:ID::NUMERIC AS market_id,
            unnested.value:Name::VARCHAR AS long_name,
            unnested.value:ShortName::VARCHAR AS short_name,
            unnested.value:URL::VARCHAR as url,
            unnested.value:Image as image
        FROM bronze.raw_data,
            LATERAL FLATTEN(jsondata) AS markets,
            LATERAL FLATTEN(markets.value:Markets) AS marketdata,
            LATERAL FLATTEN(marketdata.value) as unnested
    
        ) AS source
    ON target.market_id = source.market_id
    WHEN MATCHED AND 
        (
            source.long_name <> target.long_name OR
            source.short_name <> target.long_name OR
            source.url <> target.url OR
            source.image <> target.image
        ) 
        THEN
            UPDATE SET
            target.long_name = source.long_name,
            target.short_name = source.short_name,
            target.url = source.url,
            target.image = source.image
    WHEN NOT MATCHED
        THEN
            INSERT 
                (
                    market_id,
                    long_name,
                    short_name,
                    url,
                    image
                )
            VALUES 
                (
                    source.market_id,
                    source.long_name,
                    source.short_name,
                    source.url,
                    source.image
                )
;

