ALTER SESSION SET timezone='CET';

CREATE TEMPORARY TABLE IF NOT EXISTS temp_products (id NUMBER, productName VARCHAR,productPrice FLOAT, updatedAt timestamp);
    
INSERT INTO temp_products (id, productName,productPrice,updatedAt)
WITH p_pr_leftjoin AS (
    SELECT p.id pid, p.productname pname, pr.id prid, pr.product_name prname, p.productprice pprice, pr.product_price prprice, p.updatedAt pupdatedAt
    FROM products p  
    LEFT JOIN products_raw pr
    ON p.id = pr.id AND trim(lower(p.productname), '') = trim(lower(pr.product_name), '')
),
pr_p_leftjoin AS (
    SELECT p.id pid, p.productname pname, pr.id prid, pr.product_name prname, p.productprice pprice, pr.product_price prprice
    FROM products_raw pr
    LEFT JOIN products p
    ON p.id = pr.id AND trim(lower(p.productname), '') = trim(lower(pr.product_name), '')
)

-- It will retrieve the rows which are unchanged from the products table
SELECT pid, pname, pprice, pupdatedAt
FROM p_pr_leftjoin 
WHERE prid IS NULL
UNION 
-- It will retrieve the rows which are changed and needs to be updated in the products table 
SELECT prid, prname, prprice, current_timestamp
FROM p_pr_leftjoin 
WHERE prid IS NOT NULL
UNION 
-- It will retrieve the rows which should be newly added in products table
SELECT prid, prname, prprice, current_timestamp
FROM pr_p_leftjoin 
WHERE pid IS NULL;

TRUNCATE TABLE products;

INSERT INTO products (id,productname,productprice,updatedAt)
SELECT id,productName,productPrice,updatedAt FROM temp_products;
