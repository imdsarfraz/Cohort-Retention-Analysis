------Creating Table--------------------------------------------------------------
CREATE TABLE retail_data (
    InvoiceNo TEXT,
    StockCode TEXT,
    Description TEXT,
    Quantity INT,
    InvoiceDate TIMESTAMP,
    UnitPrice NUMERIC,
    CustomerID TEXT,
    Country TEXT
);


--------------- Importing CSV file-------------------------------------------------------------------------

COPY retail_data (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
FROM 'C:/Users/sarfr/OneDrive/Documents/GitHub/Cohort-Retention-Analysis/Retail_Data.csv'
DELIMITER ','
CSV HEADER;

SELECT * 
FROM retail_data

---- Checking Column Names and Data Types----------------
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'retail_data';

-----------Data Structure Initially
--- Total #Observations = 5,41,909
--- Total #features = 8

---------------------------------------------
-----------DATA CLEANING-----------------
-------------------------------------------------
SELECT *
FROM retail_data
WHERE customerid IS NULL;
--- # Null values Customerid =1,35,080  ==> remove these enteries because we want to check customer retention 

SELECT *
FROM retail_data
WHERE invoiceno IS NULL; --NONE 

SELECT *
FROM retail_data
WHERE country IS NULL; --NONE

SELECT *
FROM retail_data
WHERE stockcode IS NULL; --NONE

SELECT *
FROM retail_data
WHERE quantity <=0  --10,624 Entries ==> Wrong Data ==> Remove

SELECT *
FROM retail_data
WHERE unitprice <=0  -- 2517 Entries ; Prices can't be Zero or Negative 

--------DUPLICATE VALUES
SELECT InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country,
       COUNT(*) AS occurrences
FROM retail_data
GROUP BY InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;  ----4879 dupliacte values


-------Let's Store Clean Data in a CTE------

WITH raw_clean AS (
  SELECT *
  FROM retail_data
  WHERE Quantity >= 0
    AND UnitPrice >= 0
    AND CustomerID IS NOT NULL
),
clean_data AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY InvoiceNo, StockCode, CustomerID, InvoiceDate
           ORDER BY Quantity DESC, UnitPrice DESC
         ) AS rn
  FROM raw_clean
)
SELECT *
FROM clean_data
WHERE rn = 1;

---Saved file on local as .csv which i will be using later on because multiple CTE looks messy

--------------------------------------------------
------I want to check top 3 customer ID with Max revenue from each country
---------------------------------------------------
WITH raw_clean AS (
  SELECT *
  FROM retail_data
  WHERE Quantity >= 0
    AND UnitPrice >= 0
    AND CustomerID IS NOT NULL
),
clean_data AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY InvoiceNo, StockCode, CustomerID, InvoiceDate
           ORDER BY Quantity DESC, UnitPrice DESC
         ) AS rn
  FROM raw_clean
),
cust_rev AS (
  -- total revenue per customer per country
  SELECT
    customerid,
    country,
    SUM(quantity * unitprice) AS total_revenue
  FROM clean_data
  GROUP BY customerid, country
),
ranked AS (
  SELECT
    customerid,
    country,
    total_revenue,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY total_revenue DESC) AS rn
  FROM cust_rev
)

SELECT
  customerid,
  country,
  total_revenue
FROM ranked
WHERE rn <= 3
ORDER BY country, total_revenue DESC; --- country k basis pe sort then TR k basis pe sort

------------------------------------------------
---Importing Cleaned Data-----------------------
CREATE TABLE clean_data (
    InvoiceNo TEXT,
    StockCode TEXT,
    Description TEXT,
    Quantity INT,
    InvoiceDate TIMESTAMP,
    UnitPrice NUMERIC,
    CustomerID TEXT,
    Country TEXT,
	rn TEXT
);

--------------- Importing CLEANED CSV file-------------------------------------------------------------------------

COPY clean_data (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country, rn)
FROM 'C:/Users/sarfr/OneDrive/Documents/GitHub/Cohort-Retention-Analysis/clean_data.csv'
DELIMITER ','
CSV HEADER;

SELECT *
FROM clean_data;

----- Cohort Retention Analysis
SELECT MIN(InvoiceDate), MAX(InvoiceDate)
FROM clean_data;

------ the month the customer made their first purchase.
WITH first_purchase AS (
    SELECT
        CustomerID,
        MIN(DATE_TRUNC('month', InvoiceDate)) AS cohort_month ----first time purchase
    FROM clean_data
    GROUP BY CustomerID
)
SELECT *
FROM first_purchase
ORDER BY cohort_month;

---- ----------------------- ---------------------- --------------- ------------------------------------------ 
WITH first_purchase AS (
    SELECT
        CustomerID,
        MIN(DATE_TRUNC('month', InvoiceDate)) AS cohort_month  ----first time purchase
    FROM clean_data
    GROUP BY CustomerID
),
with_cohort AS (
    SELECT
        c.*,
        fp.cohort_month,
        DATE_TRUNC('month', c.InvoiceDate) AS invoice_month  ---- Next time purchase
    FROM clean_data c
    JOIN first_purchase fp
        ON c.CustomerID = fp.CustomerID
)
SELECT *
FROM with_cohort;

-------------------------------------------------------------------------------------------------------------
---------- COHORT RETENTION TABLE-----------------
WITH first_purchase AS (
    SELECT
        CustomerID,
        MIN(DATE_TRUNC('month', InvoiceDate)) AS cohort_month
    FROM clean_data
    GROUP BY CustomerID
),
with_cohort AS (
    SELECT
        c.CustomerID,
        fp.cohort_month,
        DATE_TRUNC('month', c.InvoiceDate) AS invoice_month
    FROM clean_data c
    JOIN first_purchase fp USING (CustomerID)
),
cohort_index AS (
    SELECT
        CustomerID,
        cohort_month,
        invoice_month,
        EXTRACT(YEAR FROM invoice_month) * 12 + EXTRACT(MONTH FROM invoice_month) -
        (EXTRACT(YEAR FROM cohort_month) * 12 + EXTRACT(MONTH FROM cohort_month)) + 1
        AS cohort_index
    FROM with_cohort
),
cohort_users AS (
    SELECT
        cohort_month,
        cohort_index,
        COUNT(DISTINCT CustomerID) AS users
    FROM cohort_index
    GROUP BY cohort_month, cohort_index
)
SELECT *
FROM cohort_users
ORDER BY cohort_month, cohort_index;

------------------------------
-------------Convert to a Retention Matrix (Pivot Table)
SELECT
    cohort_month,
    SUM(CASE WHEN cohort_index = 1 THEN users END) AS month_1,
    SUM(CASE WHEN cohort_index = 2 THEN users END) AS month_2,
    SUM(CASE WHEN cohort_index = 3 THEN users END) AS month_3,
    SUM(CASE WHEN cohort_index = 4 THEN users END) AS month_4,
    SUM(CASE WHEN cohort_index = 5 THEN users END) AS month_5,
    SUM(CASE WHEN cohort_index = 6 THEN users END) AS month_6
FROM (
    WITH first_purchase AS (
        SELECT CustomerID,
               MIN(DATE_TRUNC('month', InvoiceDate)) AS cohort_month
        FROM clean_data
        GROUP BY CustomerID
    ),
    with_cohort AS (
        SELECT
            c.CustomerID,
            fp.cohort_month,
            DATE_TRUNC('month', c.InvoiceDate) AS invoice_month
        FROM clean_data c
        JOIN first_purchase fp USING (CustomerID)
    ),
    cohort_index AS (
        SELECT
            CustomerID,
            cohort_month,
            invoice_month,
            EXTRACT(YEAR FROM invoice_month) * 12 + EXTRACT(MONTH FROM invoice_month) -
            (EXTRACT(YEAR FROM cohort_month) * 12 + EXTRACT(MONTH FROM cohort_month)) + 1
            AS cohort_index
        FROM with_cohort
    ),
    cohort_users AS (
        SELECT
            cohort_month,
            cohort_index,
            COUNT(DISTINCT CustomerID) AS users
        FROM cohort_index
        GROUP BY cohort_month, cohort_index
    )
    SELECT *
    FROM cohort_users
) t
GROUP BY cohort_month
ORDER BY cohort_month;
---------------------------------------------------------
------------COHORT RETENTION PERCENTAGES----------------
----------------------------------------------------------
WITH first_purchase AS (
    SELECT
        CustomerID,
        MIN(DATE_TRUNC('month', InvoiceDate)) AS cohort_month
    FROM clean_data
    GROUP BY CustomerID
),

with_cohort AS (
    SELECT
        c.CustomerID,
        fp.cohort_month,
        DATE_TRUNC('month', c.InvoiceDate) AS invoice_month
    FROM clean_data c
    JOIN first_purchase fp USING (CustomerID)
),

cohort_index AS (
    SELECT
        CustomerID,
        cohort_month,
        invoice_month,
        EXTRACT(YEAR FROM invoice_month) * 12 + EXTRACT(MONTH FROM invoice_month) -
        (EXTRACT(YEAR FROM cohort_month) * 12 + EXTRACT(MONTH FROM cohort_month)) + 1
        AS cohort_index
    FROM with_cohort
),

-- Count total customers in each cohort
cohort_size AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT CustomerID) AS total_users
    FROM cohort_index
    WHERE cohort_index = 1
    GROUP BY cohort_month
),

-- Count distinct customers returning in each cohort_index
cohort_counts AS (
    SELECT
        cohort_month,
        cohort_index,
        COUNT(DISTINCT CustomerID) AS users
    FROM cohort_index
    GROUP BY cohort_month, cohort_index
)

SELECT
    cc.cohort_month,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 1 THEN users END) / cs.total_users, 1) AS month_1,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 2 THEN users END) / cs.total_users, 1) AS month_2,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 3 THEN users END) / cs.total_users, 1) AS month_3,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 4 THEN users END) / cs.total_users, 1) AS month_4,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 5 THEN users END) / cs.total_users, 1) AS month_5,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 6 THEN users END) / cs.total_users, 1) AS month_6
FROM cohort_counts cc
JOIN JOIN cohort_size cs USING (cohort_month)
GROUP BY cc.cohort_month, cs.total_users
ORDER BY cc.cohort_month;

