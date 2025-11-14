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
