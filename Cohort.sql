-------------------------------------------------------------------------------------------------
-- COHORT RETENTION ANALYSIS (PostgreSQL)
-- Data : UCI Online Retail, sales of a UK online gift shop from 1 Dec 2010 to 9 Dec 2011
-- Steps: load the CSV -> clean -> top customers by country -> monthly cohort retention
-------------------------------------------------------------------------------------------------

------Creating Table--------------------------------------------------------------
DROP TABLE IF EXISTS retail_data;  -- so re-running the script doesn't load the data twice
CREATE TABLE retail_data (
    InvoiceNo TEXT,         -- starts with 'C' if the invoice was cancelled
    StockCode TEXT,
    Description TEXT,
    Quantity INT,
    InvoiceDate TIMESTAMP,
    UnitPrice NUMERIC,      -- price of one unit, in GBP
    CustomerID TEXT,
    Country TEXT
);


--------------- Importing CSV file-------------------------------------------------------------------------
-- Dates in the file are day-first (1/12/2010 8:26 = 1 Dec 2010). Postgres reads them month-first by
-- default, and then the COPY fails: date/time field value out of range: "13/12/2010 9:02"
SET DateStyle = 'ISO, DMY';

-- Change the path to where you saved the repo. COPY reads the file on the database server: if you get
-- "permission denied", run the same command in psql as \copy (on one line) or use pgAdmin's Import/Export.
COPY retail_data (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
FROM 'C:/Users/sarfr/OneDrive/Documents/GitHub/Cohort-Retention-Analysis/Retail_Data.csv'
DELIMITER ','
CSV HEADER;

SELECT *
FROM retail_data;

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
WHERE quantity <=0;  --10,624 Entries ==> Wrong Data ==> Remove
--- 9,288 of them are cancellations/returns (InvoiceNo starts with 'C') and the other 1,336 are stock
--- write-offs ('damaged', 'thrown away', ...) with no CustomerID, so none of them is a purchase.

SELECT *
FROM retail_data
WHERE unitprice <=0;  -- 2517 Entries ; Prices can't be Zero or Negative

--------DUPLICATE VALUES
SELECT InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country,
       COUNT(*) AS occurrences
FROM retail_data
GROUP BY InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;  ----4879 rows appear more than once ==> 5,268 extra copies to remove


-------Let's Store Clean Data in a Table------
---CREATE TABLE ... AS saves the result, so the queries below can use clean_data directly
---instead of repeating the CTEs (or exporting a .csv and importing it back).
DROP TABLE IF EXISTS clean_data;
CREATE TABLE clean_data AS
WITH raw_clean AS (
  SELECT *
  FROM retail_data
  WHERE Quantity > 0           -- > 0, not >= 0: zero is wrong data too (40 rows here have UnitPrice = 0)
    AND UnitPrice > 0
    AND CustomerID IS NOT NULL
)
-- A duplicate has the same values in all 8 columns (as in the check above), so DISTINCT removes them.
-- A ROW_NUMBER() over fewer columns, e.g. (InvoiceNo, StockCode, CustomerID, InvoiceDate), would also
-- delete 4,846 real order lines: invoice 536381 has PHOTO CLIP LINE twice, with qty 1 and qty 3.
SELECT DISTINCT *
FROM raw_clean;

SELECT COUNT(*) AS total_rows, COUNT(DISTINCT CustomerID) AS customers
FROM clean_data;  --- 3,92,692 rows, 4,338 customers

--------------------------------------------------
------I want to check top 3 customer ID with Max revenue from each country
---------------------------------------------------
-- Revenue = purchases minus cancellations, so this starts from retail_data (clean_data has no cancellations).
-- From clean_data, customer 16446 comes 3rd in the UK, but almost all of their £168k is one order of
-- 80,995 PAPER CRAFT, LITTLE BIRDIE that was cancelled 12 minutes later.
WITH sales AS (
  SELECT DISTINCT *          -- no duplicates, as in clean_data
  FROM retail_data
  WHERE CustomerID IS NOT NULL
    AND UnitPrice > 0        -- rows with Quantity < 0 (cancellations) stay in, so they get subtracted
),
cust_rev AS (
  -- total revenue per customer per country
  SELECT
    customerid,
    country,
    SUM(quantity * unitprice) AS total_revenue
  FROM sales
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
----- Cohort Retention Analysis
SELECT MIN(InvoiceDate), MAX(InvoiceDate)
FROM clean_data;  --- 2010-12-01 08:26 to 2011-12-09 12:50 ==> Dec 2011 only covers 1-9 Dec

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
    -- months since the first purchase, counting the first month as 1
    -- e.g. first purchase Dec 2010, this purchase Feb 2011: (2011*12 + 2) - (2010*12 + 12) + 1 = 3
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
----- Cohort Retention Analysis
SELECT MIN(InvoiceDate), MAX(InvoiceDate)
FROM clean_data;

---- FOR 13 Months
-------------------------
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
        (EXTRACT(YEAR FROM invoice_month) * 12 + EXTRACT(MONTH FROM invoice_month))
        - (EXTRACT(YEAR FROM cohort_month) * 12 + EXTRACT(MONTH FROM cohort_month)) + 1
        AS cohort_index
    FROM with_cohort
),

cohort_size AS (
    -- total unique users in each cohort (month 1)
    SELECT
        cohort_month,
        COUNT(DISTINCT CustomerID) AS total_users
    FROM cohort_index
    WHERE cohort_index = 1
    GROUP BY cohort_month
),

cohort_counts AS (
    -- distinct users active in each cohort_index (1..13)
    SELECT
        cohort_month,
        cohort_index,
        COUNT(DISTINCT CustomerID) AS users
    FROM cohort_index
    WHERE cohort_index BETWEEN 1 AND 13
    GROUP BY cohort_month, cohort_index
)

-- Months a cohort hasn't reached yet (e.g. month_2 of the 2011-12 cohort) are left NULL (blank), not 0.0,
-- so they don't read as "nobody came back".
-- The last value in each row is Dec 2011, which only has data up to 9 Dec, so it is lower than it should be.
SELECT
    TO_CHAR(cc.cohort_month, 'YYYY-MM') AS cohort_month,
    cs.total_users AS customers,  -- cohort size: customers whose first purchase was in that month
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 1 THEN users END) / cs.total_users, 1) AS month_1,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 2 THEN users END) / cs.total_users, 1) AS month_2,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 3 THEN users END) / cs.total_users, 1) AS month_3,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 4 THEN users END) / cs.total_users, 1) AS month_4,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 5 THEN users END) / cs.total_users, 1) AS month_5,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 6 THEN users END) / cs.total_users, 1) AS month_6,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 7 THEN users END) / cs.total_users, 1) AS month_7,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 8 THEN users END) / cs.total_users, 1) AS month_8,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 9 THEN users END) / cs.total_users, 1) AS month_9,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 10 THEN users END) / cs.total_users, 1) AS month_10,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 11 THEN users END) / cs.total_users, 1) AS month_11,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 12 THEN users END) / cs.total_users, 1) AS month_12,
    ROUND(100.0 * SUM(CASE WHEN cohort_index = 13 THEN users END) / cs.total_users, 1) AS month_13
FROM cohort_counts cc
JOIN cohort_size cs USING (cohort_month)
GROUP BY cc.cohort_month, cs.total_users
ORDER BY cc.cohort_month;

