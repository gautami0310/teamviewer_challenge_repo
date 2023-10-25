-- Needs to alter the session so that we get timestamp in CET while Executing the queries.
ALTER SESSION SET timezone='CET';

--Creating a temporary table for holding intermediate data. Here I check for the columns whose values have been changed.
CREATE TEMPORARY TABLE IF NOT EXISTS tempTBL (id NUMBER, productName VARCHAR,productPrice FLOAT);
    
INSERT INTO tempTBL (id, productName,productPrice)
    
WITH prod_raw AS (SELECT    id
                               ,product_name
                               ,product_price
                               ,HASH(id,product_name,product_price) AS hash_key_prod_raw
                      FROM PRODUCTS_RAW
 ),
    
prod_tbl AS ( SELECT id
                         ,productName
                         ,productPrice
                         ,HASH(id,productName,productPrice) AS hash_key_prod_tbl
                   FROM PRODUCTS
),
    
change_data_ids AS ( SELECT    prdR.id AS id_from_prdRAW
                                  ,prdT.id AS id_from_prdTBL
                                  ,prdR.hash_key_prod_raw AS hash_from_prdRAW
                                  ,prdT.hash_key_prod_tbl AS hash_from_prdTBL
                         FROM prod_raw prdR 
                         LEFT JOIN prod_tbl prdT 
                         ON prdR.id = prdT.id AND prdR.hash_key_prod_raw = prdT.hash_key_prod_tbl
 ),

    
final_new_changed_data AS (SELECT   prdR.id 
                                       ,CASE WHEN prdR.product_name = prdT.PRODUCTNAME THEN NULL ELSE prdR.product_name END AS PRODUCTNAME_NEW
                                       ,CASE WHEN prdR.product_price = prdT.PRODUCTPRICE THEN NULL ELSE prdR.product_price END AS PRODUCTPRICE_NEW
                               FROM PRODUCTS_RAW prdR 
                               LEFT JOIN PRODUCTS prdT 
                               ON prdR.id = prdT.id 
                               WHERE prdR.id IN (SELECT id_from_prdRAW FROM change_data_ids WHERE id_from_prdTBL IS NULL)
)
    
SELECT final_new_changed_data.id,final_new_changed_data.PRODUCTNAME_NEW,final_new_changed_data.PRODUCTPRICE_NEW
FROM final_new_changed_data;
    

-- Update the 'products' table with new values and also insert new rows for new product IDs.    
MERGE INTO products AS target
    USING tempTBL AS source
    ON target.id = source.id
    WHEN MATCHED
        THEN UPDATE SET target.PRODUCTNAME = (CASE WHEN (source.PRODUCTNAME IS NOT NULL) THEN source.PRODUCTNAME ELSE target.PRODUCTNAME END)
        ,target.PRODUCTPRICE = (CASE WHEN (source.PRODUCTPRICE IS NOT NULL) THEN source.PRODUCTPRICE ELSE target.PRODUCTPRICE END)
        ,target.updatedAt =CURRENT_TIMESTAMP 
    WHEN NOT MATCHED THEN
        INSERT (ID,PRODUCTNAME, PRODUCTPRICE ,updatedAt) VALUES (source.ID,source.PRODUCTNAME, source.PRODUCTPRICE,CURRENT_TIMESTAMP);


TRUNCATE TABLE tempTBL; -- once the purpose is done needs to be truncated  
    




