-- =============================================================================================
-- COHORT RETENTION ANALYSIS (PostgreSQL)
--
-- Question: customers are grouped by the month of their first purchase (their "cohort").
--           What share of each cohort buys again 1, 2, 3, ... months later?
-- Data    : UCI Online Retail, sales of a UK online gift shop from 1 Dec 2010 to 9 Dec 2011
--
-- Steps   : 0. reset   1. load   2. check   3. clean   4. top customers by country
--           5. cohorts   6. retention   7. retention matrix (the final result)
-- Steps 5-7 are saved as views, so you can look at any of them on its own, e.g.
--   SELECT * FROM customer_months LIMIT 20;
-- =============================================================================================


-- 0. Start from scratch, so the whole script can be run again ---------------------------------
DROP VIEW IF EXISTS retention_matrix, cohort_retention, customer_months;
DROP TABLE IF EXISTS clean_data, retail_data;


-- 1. Load the raw data ------------------------------------------------------------------------
CREATE TABLE retail_data (
    invoice_no   TEXT,         -- starts with 'C' if the invoice was cancelled
    stock_code   TEXT,         -- product code
    description  TEXT,
    quantity     INTEGER,      -- negative for cancellations
    invoice_date TIMESTAMP,
    unit_price   NUMERIC,      -- price of one unit, in GBP
    customer_id  INTEGER,
    country      TEXT
);

-- The dates in the file are day-first (1/12/2010 8:26 = 1 Dec 2010), but Postgres reads them
-- month-first by default and the COPY fails at "13/12/2010". This setting lasts for the session.
SET DateStyle = 'ISO, DMY';

-- Change the path to where you saved the repo. COPY reads the file on the database server: if you
-- get "permission denied", use psql's \copy instead (see the README) or pgAdmin's Import/Export.
COPY retail_data
FROM 'C:/Users/sarfr/OneDrive/Documents/GitHub/Cohort-Retention-Analysis/Retail_Data.csv'
WITH (FORMAT csv, HEADER true);

SELECT * FROM retail_data LIMIT 5;


-- 2. Check the data quality -------------------------------------------------------------------
-- One row with the size of each problem. COUNT(*) FILTER (WHERE ...) only counts the rows that match.
SELECT
    COUNT(*)                                                                     AS total_rows,
    COUNT(*) FILTER (WHERE customer_id IS NULL)                                  AS no_customer_id,
    COUNT(*) FILTER (WHERE quantity <= 0)                                        AS quantity_not_positive,
    COUNT(*) FILTER (WHERE invoice_no LIKE 'C%')                                 AS cancelled,
    COUNT(*) FILTER (WHERE unit_price <= 0)                                      AS price_not_positive,
    COUNT(*) FILTER (WHERE invoice_no IS NULL OR stock_code IS NULL OR country IS NULL) AS other_missing,
    COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM retail_data) AS d)  AS duplicate_rows
FROM retail_data;
-- total_rows 541,909 | no_customer_id 135,080 | quantity_not_positive 10,624 | cancelled 9,288
-- price_not_positive 2,517 | other_missing 0 | duplicate_rows 5,268
-- The 1,336 rows with quantity <= 0 that weren't cancelled are stock write-offs ('damaged', ...).


-- 3. Clean ------------------------------------------------------------------------------------
-- Keep the purchases of known customers, and one copy of each duplicated row.
CREATE TABLE clean_data AS
SELECT DISTINCT *                -- a duplicate has the same values in all 8 columns
FROM retail_data
WHERE customer_id IS NOT NULL    -- a customer without an ID can't be followed over time
  AND quantity > 0               -- drops cancellations
  AND unit_price > 0;            -- drops free items (40 rows have a price of 0)

SELECT
    COUNT(*)                    AS total_rows,
    COUNT(DISTINCT customer_id) AS customers,
    MIN(invoice_date)           AS first_sale,
    MAX(invoice_date)           AS last_sale
FROM clean_data;
-- 392,692 rows and 4,338 customers, from 2010-12-01 08:26 to 2011-12-09 12:50.
-- The data stops on 9 Dec 2011, so December 2011 is an incomplete month.


-- 4. Top 3 customers by revenue in each country -----------------------------------------------
-- Revenue = purchases minus cancellations, so this starts from retail_data (clean_data has no
-- cancellations). Counting purchases only, customer 16446 would come 3rd in the UK because of
-- one order of 80,995 items that was cancelled 12 minutes later.
WITH customer_revenue AS (
    SELECT
        country,
        customer_id,
        SUM(quantity * unit_price) AS revenue
    FROM (SELECT DISTINCT * FROM retail_data) AS no_duplicates
    WHERE customer_id IS NOT NULL
      AND unit_price > 0
    GROUP BY country, customer_id
    HAVING SUM(quantity * unit_price) > 0    -- leave out customers whose cancellations cancel out their purchases
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY revenue DESC, customer_id) AS rank_in_country
    FROM customer_revenue
)
SELECT country, rank_in_country, customer_id, revenue
FROM ranked
WHERE rank_in_country <= 3
ORDER BY country, rank_in_country;
-- e.g. United Kingdom: 18102 (256,438.49), 17450 (187,322.17) and 17511 (88,125.38)


-- 5. Cohorts ----------------------------------------------------------------------------------
-- One row per customer per month in which they bought something:
--   cohort_month = month of the customer's first purchase
--   month_number = months since then, 0 for the first month
CREATE VIEW customer_months AS
WITH months AS (
    SELECT DISTINCT
        customer_id,
        DATE_TRUNC('month', invoice_date)::date AS order_month
    FROM clean_data
),
with_cohort AS (
    SELECT
        customer_id,
        MIN(order_month) OVER (PARTITION BY customer_id) AS cohort_month,  -- the customer's earliest month
        order_month
    FROM months
)
SELECT
    customer_id,
    cohort_month,
    order_month,
    -- whole months between the two: years apart * 12 + months apart
    -- e.g. Dec 2010 -> Feb 2011: (2011 - 2010) * 12 + (2 - 12) = 2
    ((EXTRACT(YEAR FROM order_month) - EXTRACT(YEAR FROM cohort_month)) * 12
      + EXTRACT(MONTH FROM order_month) - EXTRACT(MONTH FROM cohort_month))::int AS month_number
FROM with_cohort;

-- Example: customer 12347 first bought in Dec 2010 and came back in 6 other months
SELECT * FROM customer_months WHERE customer_id = 12347 ORDER BY order_month;


-- 6. Retention --------------------------------------------------------------------------------
-- For each cohort and month: how many of the cohort's customers bought, and what % of the cohort that is.
CREATE VIEW cohort_retention AS
WITH counts AS (
    SELECT
        cohort_month,
        month_number,
        COUNT(*) AS customers    -- customer_months has one row per customer per month
    FROM customer_months
    GROUP BY cohort_month, month_number
)
SELECT
    cohort_month,
    month_number,
    customers,
    -- divide by the month-0 count, which is the cohort size (everyone buys in their first month)
    ROUND(100.0 * customers
          / FIRST_VALUE(customers) OVER (PARTITION BY cohort_month ORDER BY month_number), 1) AS retention_pct
FROM counts;

-- Example: the January 2011 cohort, month by month
SELECT * FROM cohort_retention WHERE cohort_month = '2011-01-01' ORDER BY month_number;


-- 7. Retention matrix: one row per cohort, one column per month -------------------------------
-- MAX(...) FILTER (WHERE month_number = k) picks the cohort's value for month k (there is only one).
-- Months a cohort hasn't reached yet stay NULL, e.g. month_1 of the Dec 2011 cohort.
-- The last value in each row is December 2011, which only has data up to the 9th, so it is too low.
CREATE VIEW retention_matrix AS
SELECT
    TO_CHAR(r.cohort_month, 'YYYY-MM')                  AS cohort_month,
    MAX(customers)     FILTER (WHERE month_number = 0)  AS customers,
    MAX(retention_pct) FILTER (WHERE month_number = 0)  AS month_0,
    MAX(retention_pct) FILTER (WHERE month_number = 1)  AS month_1,
    MAX(retention_pct) FILTER (WHERE month_number = 2)  AS month_2,
    MAX(retention_pct) FILTER (WHERE month_number = 3)  AS month_3,
    MAX(retention_pct) FILTER (WHERE month_number = 4)  AS month_4,
    MAX(retention_pct) FILTER (WHERE month_number = 5)  AS month_5,
    MAX(retention_pct) FILTER (WHERE month_number = 6)  AS month_6,
    MAX(retention_pct) FILTER (WHERE month_number = 7)  AS month_7,
    MAX(retention_pct) FILTER (WHERE month_number = 8)  AS month_8,
    MAX(retention_pct) FILTER (WHERE month_number = 9)  AS month_9,
    MAX(retention_pct) FILTER (WHERE month_number = 10) AS month_10,
    MAX(retention_pct) FILTER (WHERE month_number = 11) AS month_11,
    MAX(retention_pct) FILTER (WHERE month_number = 12) AS month_12
FROM cohort_retention AS r
GROUP BY r.cohort_month;

-- The final result, saved as "Cohort Retention Data.csv"
SELECT * FROM retention_matrix ORDER BY cohort_month;
