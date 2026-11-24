-- =============================================================================================
-- COHORT RETENTION ANALYSIS (PostgreSQL)
--
-- Question: customers are grouped by the month of their first purchase (their "cohort").
--           What share of each cohort buys again 1, 2, 3, ... months later?
-- Data    : UCI Online Retail, sales of a UK online gift shop from 1 Dec 2010 to 9 Dec 2011
--
-- Key terms
--   cohort        customers whose first purchase was in the same month. The "Jan 2011 cohort"
--                 is everyone who bought for the first time in January 2011.
--   cohort_month  the month of a customer's first purchase, i.e. the cohort they belong to
--   month_number  months since the first purchase: 0 = the month of the first purchase,
--                 1 = the month after it, and so on
--   retention     the % of a cohort that buys something in a later month
--
-- Steps   : 0. reset   1. load   2. check   3. clean   4. top customers by country
--           5. cohorts   6. retention   7. retention matrix (the final result)
-- Steps 5-7 are saved as views, so you can look at any of them on its own, e.g.
--   SELECT * FROM customer_months LIMIT 20;
--
-- Reading this file: lines starting with -- are comments, which Postgres ignores. Each step says
-- what its columns mean ("Columns"), explains the SQL it uses for the first time ("SQL"), gives
-- its result with how to read it ("Result") and warns about mistakes beginners often make there
-- ("Mistake"). The README lists all the mistakes in one place.
-- =============================================================================================


-- 0. Start from scratch, so the whole script can be run again ---------------------------------
-- SQL: DROP VIEW / DROP TABLE delete a view or a table. IF EXISTS skips the ones that don't exist
--      yet instead of stopping with an error. The views go first because they're built on the tables.
-- Mistake: running the COPY in step 1 again without dropping the table first. It adds every row a
--          second time and doubles all the counts.
DROP VIEW IF EXISTS retention_matrix, cohort_retention, customer_months;
DROP TABLE IF EXISTS clean_data, retail_data;


-- 1. Load the raw data ------------------------------------------------------------------------
-- retail_data holds the CSV as it is: one row per product line of an invoice, so an invoice for
-- 3 different products is 3 rows.
-- SQL: CREATE TABLE makes an empty table and sets the type of each column: TEXT = any text,
--      INTEGER = a whole number, NUMERIC = an exact decimal number (right for money),
--      TIMESTAMP = a date and time.
CREATE TABLE retail_data (
    invoice_no   TEXT,         -- invoice number; starts with 'C' if the invoice was cancelled
    stock_code   TEXT,         -- product code, e.g. 85123A
    description  TEXT,         -- product name, e.g. WHITE HANGING HEART T-LIGHT HOLDER
    quantity     INTEGER,      -- units on this line; negative for cancellations
    invoice_date TIMESTAMP,    -- date and time of the invoice
    unit_price   NUMERIC,      -- price of one unit, in GBP
    customer_id  INTEGER,      -- customer number; missing (NULL) on a quarter of the rows
    country      TEXT          -- the customer's country
);

-- SQL: SET DateStyle tells Postgres how to read dates. The dates in the file are day-first
--      (1/12/2010 8:26 = 1 Dec 2010), but Postgres reads them month-first by default and the
--      COPY fails at "13/12/2010". The setting lasts until you disconnect.
-- Mistake: not checking how the dates are written. Here the COPY fails, which is easy to notice,
--          but in a file where no day is above 12, every date would load with the day and month
--          swapped and no error. Check the first and last dates after loading (step 3 does).
SET DateStyle = 'ISO, DMY';

-- SQL: COPY loads a file into a table. FORMAT csv = the values are separated by commas,
--      HEADER true = skip the first line, which has the column names.
-- Change the path to where you saved the repo. COPY reads the file on the database server: if you
-- get "permission denied", use psql's \copy instead (see the README) or pgAdmin's Import/Export.
COPY retail_data
FROM 'C:/Users/sarfr/OneDrive/Documents/GitHub/Cohort-Retention-Analysis/Retail_Data.csv'
WITH (FORMAT csv, HEADER true);

-- SQL: SELECT * FROM t shows every column (*) of table t. LIMIT 5 stops after 5 rows.
SELECT * FROM retail_data LIMIT 5;


-- 2. Check the data quality -------------------------------------------------------------------
-- One row that measures every problem in the raw data.
-- Columns:
--   total_rows             all the rows in the file
--   no_customer_id         rows without a customer number: we can't follow these buyers over time
--   quantity_not_positive  rows with 0 or fewer units: cancellations and stock write-offs
--   cancelled              rows of cancelled invoices (invoice_no starts with 'C')
--   price_not_positive     rows with a price of 0 or less: free items and accounting adjustments
--   other_missing          rows without an invoice number, product code or country
--   duplicate_rows         extra copies of rows that appear more than once
-- SQL: COUNT(*) counts rows. COUNT(*) FILTER (WHERE condition) counts only the rows where the
--      condition is true, so many different counts fit in one query.
--      AS gives a column (or a table) a name.
--      IS NULL = the value is missing. LIKE 'C%' = the text starts with C (% stands for "anything").
--      A query in brackets is a subquery: Postgres runs it first and uses its result.
--      SELECT DISTINCT * keeps one copy of each set of identical rows, so
--      all rows - distinct rows = the extra copies.
-- Mistake: writing customer_id = NULL instead of customer_id IS NULL. Nothing is equal to NULL, not
--          even NULL, so "= NULL" matches no rows and would report 0 missing customer IDs.
SELECT
    COUNT(*)                                                                     AS total_rows,
    COUNT(*) FILTER (WHERE customer_id IS NULL)                                  AS no_customer_id,
    COUNT(*) FILTER (WHERE quantity <= 0)                                        AS quantity_not_positive,
    COUNT(*) FILTER (WHERE invoice_no LIKE 'C%')                                 AS cancelled,
    COUNT(*) FILTER (WHERE unit_price <= 0)                                      AS price_not_positive,
    COUNT(*) FILTER (WHERE invoice_no IS NULL OR stock_code IS NULL OR country IS NULL) AS other_missing,
    COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM retail_data) AS d)  AS duplicate_rows
FROM retail_data;
-- Result: total_rows 541,909 | no_customer_id 135,080 | quantity_not_positive 10,624
--         cancelled 9,288 | price_not_positive 2,517 | other_missing 0 | duplicate_rows 5,268
-- The 1,336 rows with quantity <= 0 that weren't cancelled are stock write-offs ('damaged', ...).


-- 3. Clean ------------------------------------------------------------------------------------
-- clean_data keeps the purchases of known customers, with one copy of each duplicated row.
-- It has the same 8 columns as retail_data.
-- SQL: CREATE TABLE ... AS saves the result of a query as a new table, so the later steps can use
--      it without repeating the query. WHERE keeps only the rows where the conditions are true
--      (AND = both must be true).
-- Mistakes:
--   >= 0 instead of > 0: unit_price >= 0 would keep the 40 free items, which aren't purchases.
--   Removing duplicates on a few columns only: keeping one row per invoice_no, stock_code,
--   customer_id and invoice_date (e.g. with ROW_NUMBER()) also deletes 4,846 real order lines.
--   Invoice 536381 has PHOTO CLIP LINE twice, with quantities 1 and 3. And if you number the
--   copies with ROW_NUMBER() ... AS rn, they're only removed once you keep WHERE rn = 1.
--   Exporting the result to a CSV and loading it back instead of saving it as a table: the file can
--   change on the way (535 rows here have a " in the product name, e.g. RECORD FRAME 7" SINGLE SIZE)
--   and goes out of date when the query changes.
CREATE TABLE clean_data AS
SELECT DISTINCT *                -- a duplicate has the same values in all 8 columns
FROM retail_data
WHERE customer_id IS NOT NULL    -- a customer without an ID can't be followed over time
  AND quantity > 0               -- drops cancellations
  AND unit_price > 0;            -- drops free items (40 rows have a price of 0)

-- Columns: total_rows = rows left after cleaning, customers = number of different customers,
--          first_sale / last_sale = date and time of the earliest and the latest sale
-- SQL: COUNT(DISTINCT x) counts the different values of x. MIN and MAX give the smallest and the
--      largest value; for dates, the earliest and the latest.
SELECT
    COUNT(*)                    AS total_rows,
    COUNT(DISTINCT customer_id) AS customers,
    MIN(invoice_date)           AS first_sale,
    MAX(invoice_date)           AS last_sale
FROM clean_data;
-- Result: 392,692 rows and 4,338 customers, from 2010-12-01 08:26 to 2011-12-09 12:50.
-- The data stops on 9 Dec 2011, so December 2011 is an incomplete month.


-- 4. Top 3 customers by revenue in each country -----------------------------------------------
-- Revenue = purchases minus cancellations, so this starts from retail_data (clean_data has no
-- cancellations).
-- Columns:
--   country          the customer's country
--   rank_in_country  1 = the customer with the most revenue in that country, 2 = the next, ...
--   customer_id      the customer
--   revenue          what the customer bought minus what they cancelled, in GBP
-- SQL: WITH name AS (query) defines a CTE (common table expression): a named step that the rest
--      of the query can use like a table. customer_revenue is used by ranked, and ranked by the
--      final SELECT, so the query reads from top to bottom instead of as queries nested in brackets.
--      GROUP BY country, customer_id makes one group per customer and country, and
--      SUM(quantity * unit_price) adds up the amounts in each group. HAVING then filters whole
--      groups (WHERE filters single rows, before they're grouped).
--      ROW_NUMBER() OVER (PARTITION BY country ORDER BY revenue DESC, customer_id) is a window
--      function: it numbers the customers 1, 2, 3, ... separately for each country (PARTITION BY),
--      from the highest revenue down (DESC = descending), with customer_id breaking ties. Unlike
--      GROUP BY, a window function keeps every row. WHERE can't use its result in the same SELECT,
--      which is why the ranking is its own step.
--      ORDER BY sorts the final result.
-- Mistake: computing revenue from clean_data, i.e. ignoring the cancellations. Customer 16446
--          would then come 3rd in the UK, because of one order of 80,995 items that was cancelled
--          12 minutes later. For retention it's the other way round: a cancellation isn't a
--          purchase, so clean_data rightly leaves them out.
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
-- Result: up to 3 customers per country (89 rows). For example, in the United Kingdom customer
-- 18102 brought in the most revenue (256,438.49 GBP), then 17450 (187,322.17) and 17511 (88,125.38).


-- 5. Cohorts ----------------------------------------------------------------------------------
-- customer_months: one row per customer per month in which they bought something.
-- Columns:
--   customer_id   the customer
--   cohort_month  the month of the customer's first purchase, i.e. their cohort
--                 (a month is stored as its first day: 2011-01-01 = January 2011)
--   order_month   a month in which the customer bought something
--   month_number  months from cohort_month to order_month: 0 = the month of the first purchase,
--                 1 = the month after it, ...
-- SQL: CREATE VIEW saves a query under a name. A view stores no data: every time you use it,
--      Postgres runs its query again, so it always matches clean_data.
--      DATE_TRUNC('month', x) rounds a date and time down to the start of its month
--      (2011-01-17 14:30 -> 2011-01-01 00:00). :: converts a value to another type: ::date keeps
--      just the date, ::int makes a whole number.
--      MIN(order_month) OVER (PARTITION BY customer_id) is a window function: on every row it gives
--      the earliest order_month of the same customer, without merging the rows.
--      EXTRACT(YEAR FROM x) and EXTRACT(MONTH FROM x) take the year (e.g. 2011) or the month
--      (1 to 12) out of a date.
-- Mistakes:
--   Counting rows instead of customers: an invoice has one row per product, so COUNT(*) on the sales
--   counts product lines. In January 2011 the cohort's 417 new customers bought 10,877 product lines.
--   Count distinct customers, or first make one row per customer per month, as this view does.
--   Subtracting the months without the years: Dec 2010 -> Feb 2011 would be 2 - 12 = -10 months.
CREATE VIEW customer_months AS
WITH months AS (
    -- one row per customer per month: DISTINCT keeps a single row when a customer bought
    -- several times in the same month
    SELECT DISTINCT
        customer_id,
        DATE_TRUNC('month', invoice_date)::date AS order_month
    FROM clean_data
),
with_cohort AS (
    -- adds the customer's first month (their cohort) to each of their rows
    SELECT
        customer_id,
        MIN(order_month) OVER (PARTITION BY customer_id) AS cohort_month,
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

-- Example: customer 12347
SELECT * FROM customer_months WHERE customer_id = 12347 ORDER BY order_month;
-- Result: 7 rows, all with cohort_month 2010-12-01: 12347 first bought in December 2010 (month 0)
-- and came back in months 1, 4, 6, 8, 10 and 12. For example, the row with order_month 2011-04-01
-- and month_number 4 means they bought in April 2011, 4 months after their first purchase.


-- 6. Retention --------------------------------------------------------------------------------
-- cohort_retention: one row per cohort and month.
-- Columns:
--   cohort_month   the cohort
--   month_number   months since the first purchase
--   customers      how many of the cohort's customers bought something in that month
--   retention_pct  customers as a % of the cohort size
-- The cohort size is the month-0 count, because every customer buys in their own first month.
-- SQL: FIRST_VALUE(customers) OVER (PARTITION BY cohort_month ORDER BY month_number) is a window
--      function: on every row it gives the customers value of the first row of the same cohort,
--      in month_number order, i.e. the month-0 count.
--      ROUND(x, 1) rounds to 1 decimal place.
-- Mistake: customers / cohort size * 100 gives 0 here, because dividing two whole numbers drops the
--          decimals in Postgres (92 / 417 = 0). Starting with 100.0 makes it a decimal division:
--          100.0 * 92 / 417 = 22.06.
CREATE VIEW cohort_retention AS
WITH counts AS (
    -- customer_months has one row per customer per month, so COUNT(*) counts customers
    SELECT
        cohort_month,
        month_number,
        COUNT(*) AS customers
    FROM customer_months
    GROUP BY cohort_month, month_number
)
SELECT
    cohort_month,
    month_number,
    customers,
    ROUND(100.0 * customers
          / FIRST_VALUE(customers) OVER (PARTITION BY cohort_month ORDER BY month_number), 1) AS retention_pct
FROM counts;

-- Example: the January 2011 cohort, month by month
SELECT * FROM cohort_retention WHERE cohort_month = '2011-01-01' ORDER BY month_number;
-- Result: 417 customers in month 0 (the cohort size), 92 in month 1, 111 in month 2, ...
-- For example, the row with month_number 1, customers 92 and retention_pct 22.1 means 92 of the
-- 417 customers (22.1%) bought again in February 2011.


-- 7. Retention matrix: one row per cohort, one column per month -------------------------------
-- retention_matrix: the numbers of cohort_retention laid out as a grid (a "pivot"). This is the
-- table behind the heatmap in the README.
-- Columns:
--   cohort_month          the cohort, as text: '2011-01' = customers who first bought in January 2011
--   customers             the cohort size
--   month_0 ... month_12  retention_pct in each month since the first purchase; month_0 is always 100
-- SQL: GROUP BY r.cohort_month makes one row per cohort. MAX(retention_pct) FILTER (WHERE
--      month_number = 1) takes the cohort's value for month 1 (there's only one, so MAX simply
--      picks it), and the same for every other month.
--      NULL means "no value": a month that a cohort hasn't reached yet has no row in
--      cohort_retention, so its column is NULL.
--      TO_CHAR(date, 'YYYY-MM') writes a date as text, e.g. 2011-01-01 -> '2011-01'.
--      AS r gives cohort_retention the short name r. r.cohort_month is its date column, not the
--      text column with the same name that TO_CHAR makes.
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
-- Result: 13 rows, one per cohort from 2010-12 to 2011-12. How to read it: in the 2011-01 row,
-- month_1 = 22.1 means 22.1% of January's new customers bought again in February, and
-- month_3 = 23.0 means 23.0% bought in April (3 months after January).
-- Blank (NULL) = the cohort hasn't reached that month yet, e.g. month_1 of the 2011-12 cohort.
-- Mistakes when reading it:
--   Filling the blanks with 0 (e.g. with COALESCE(..., 0)): 0% reads as "nobody came back", when the
--   month just hasn't happened yet.
--   Reading the last value of each row as customers leaving: it's December 2011, which only has data
--   up to the 9th, so it is too low (the 2011-02 cohort goes from 30.5% in November to 6.8%).
--   Comparing the first cohort with the others: the data starts on 1 Dec 2010, so the 2010-12 cohort
--   includes customers who were already buying before that, and looks far more loyal (32% to 50%).
