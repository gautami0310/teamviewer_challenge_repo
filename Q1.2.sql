-- Needs to alter the session so that we get timestamp in CET while Executing the queries.
ALTER SESSION SET timezone='CET';

-- We can directly enter new lines for the IDs whose data is changed into 'products_history' table.   
INSERT INTO products_history (ID,PRODUCTNAME, PRODUCTPRICE ,RECORDS_VALID_FROM,records_valid_to)
SELECT DISTINCT prdR.id 
                   ,prdR.product_name  AS PRODUCTNAME_NEW
                   ,prdR.product_price AS PRODUCTPRICE_NEW
                   ,CURRENT_TIMESTAMP::timestamp
                   ,NULL
FROM PRODUCTS_RAW prdR 
LEFT JOIN PRODUCTS_history prdH 
ON prdR.id = prdH.id
WHERE HASH(prdR.id,prdR.product_Name,prdR.product_Price) != HASH(prdH.id,prdH.productName,prdH.productPrice) AND prdH.RECORDS_VALID_TO IS NULL;

--Creating a temporary table for holding intermediate data. 
CREATE TEMPORARY TABLE IF NOT EXISTS history_updation (latest_records_valid_from TIMESTAMP,id NUMERIC,immediate_old_records_valid_from TIMESTAMP);

INSERT INTO history_updation (latest_records_valid_from,id,immediate_old_records_valid_from)
WITH latest_time AS (    SELECT DISTINCT id, LEAD(records_valid_from) OVER (PARTITION BY id ORDER BY records_valid_from ASC) AS latest_record 
                             FROM products_history 
                             WHERE records_valid_to IS NULL 
                             QUALIFY latest_record IS NOT NULL
 ),
    
 rank_for_old_records AS (SELECT DISTINCT id, RANK() OVER (PARTITION BY id ORDER BY records_valid_from DESC) AS rank_1 
                                    ,records_valid_to 
                                    ,records_valid_from 
                             FROM products_history 
                             WHERE records_valid_to IS NULL 
                             QUALIFY rank_1 = 2 )

SELECT DISTINCT latest_record 
            ,lt.id
            ,rr.records_valid_from 
FROM latest_time lt 
LEFT JOIN rank_for_old_records rr 
ON lt.id=rr.id;


-- Updating the timestamp for 'records_valid_to' column of the old lines in 'products_history' table
MERGE INTO products_history AS target
    USING history_updation AS source
    ON target.id = source.id AND target.records_valid_from = source.immediate_old_records_valid_from
    WHEN MATCHED 
        THEN UPDATE SET target.records_valid_to = source.LATEST_RECORDS_VALID_FROM;

    
TRUNCATE TABLE history_updation;
    
    



