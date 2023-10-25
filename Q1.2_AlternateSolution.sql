ALTER SESSION SET timezone='CET';

CREATE TEMPORARY TABLE IF NOT EXISTS temp_products_his (id NUMBER, productName VARCHAR,productPrice FLOAT, valid_from timestamp, valid_to timestamp);
    
INSERT INTO temp_products_his (id, productName,productPrice,valid_from, valid_to)
WITH p_pr_leftjoin AS (
    SELECT p.id pid, p.productname pname, pr.id prid, pr.product_name prname, p.productprice pprice, pr.product_price prprice, p.records_valid_from valid_from, p.records_valid_to as valid_to
    FROM products_history p  
    LEFT JOIN products_raw pr
    ON p.id = pr.id AND trim(lower(p.productname), '') = trim(lower(pr.product_name), '')
),
pr_p_leftjoin AS (
    SELECT p.id pid, p.productname pname, pr.id prid, pr.product_name prname, p.productprice pprice, pr.product_price prprice
    FROM products_raw pr
    LEFT JOIN products_history p
    ON p.id = pr.id AND trim(lower(p.productname), '') = trim(lower(pr.product_name), '')
)

-- It will retrieve the rows which are changed and needs to be updated in the products table 
SELECT prid, prname, prprice, current_timestamp AS VALID_FROM, '9999-12-31 23:59:59.000' AS VALID_TO
FROM p_pr_leftjoin 
WHERE prid IS NOT NULL
UNION 
-- It will retrieve the rows which should be newly added in products table
SELECT prid, prname, prprice, current_timestamp AS VALID_FROM, '9999-12-31 23:59:59.000' AS VALID_TO
FROM pr_p_leftjoin 
WHERE pid IS NULL;

UPDATE PRODUCTS_HISTORY PH SET RECORDS_VALID_TO = (SELECT COALESCE(MAX(TPH.VALID_FROM),'9999-12-31 23:59:59.000') FROM TEMP_PRODUCTS_HIS TPH
WHERE PH.ID = TPH.ID)

INSERT INTO PRODUCTS_HISTORY (id,productname,productprice,RECORDS_VALID_FROM, RECORDS_VALID_TO)
SELECT id, productName,productPrice,valid_from, valid_to FROM TEMP_PRODUCTS_HIS;

