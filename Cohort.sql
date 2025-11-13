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

-------Let's Store Clean Data in a CTE------
WITH clean_data AS (
SELECT *
FROM retail_data 
WHERE Quantity >=0 AND UnitPrice >=0  AND CustomerID IS NOT NULL)
--- Lets see which country has highest Total Revenue
SELECT DISTINCT ON (country)
  country,
  customerid,
  (Quantity * UnitPrice) AS Total_revenue
FROM clean_data
ORDER BY country, Total_revenue DESC;

--------------------------------------------------
------I want to check top 3 customer ID with Max revenue from each country
---------------------------------------------------
WITH clean_data AS (
  SELECT *
  FROM retail_data
  WHERE Quantity >= 0
    AND UnitPrice >= 0
    AND CustomerID IS NOT NULL
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

