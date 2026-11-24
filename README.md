# Cohort Retention Analysis (SQL)

How many customers come back after their first order? This project groups the customers of a UK online gift shop into **monthly cohorts** (the month of their first purchase) and uses **PostgreSQL** to measure what share of each cohort orders again in each of the following months.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="images/cohort_retention_heatmap_dark.png">
    <img src="images/cohort_retention_heatmap.png" alt="Heatmap of monthly cohort retention: one row per cohort from Dec 2010 to Dec 2011, one column per month since the first purchase" width="760">
  </picture>
</p>

## Dataset

[Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail) from the UCI Machine Learning Repository: every transaction of a UK-based online shop between **1 Dec 2010 and 9 Dec 2011**. The shop mainly sells all-occasion gifts, and many of its customers are wholesalers. There are 541,909 rows, one per product line of an invoice:

| Column | Description |
| --- | --- |
| `InvoiceNo` | Invoice number. Starts with `C` if the invoice was cancelled |
| `StockCode` | Product code |
| `Description` | Product name |
| `Quantity` | Units on this line (negative for cancellations) |
| `InvoiceDate` | Date and time of the invoice. Day-first in this CSV: `1/12/2010 8:26` is 1 Dec 2010 |
| `UnitPrice` | Price of one unit, in GBP |
| `CustomerID` | Customer number. Missing on a quarter of the rows |
| `Country` | Customer's country |

Chen, D. (2015). *Online Retail* [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5BW33 (CC BY 4.0).

## What's in this repo

| File | Description |
| --- | --- |
| [`Cohort.sql`](Cohort.sql) | All the SQL: load the CSV, clean it, top customers per country, cohort retention |
| [`Retail_Data.csv`](Retail_Data.csv) | Raw data (541,909 rows) |
| [`Cohort Retention Data.csv`](Cohort%20Retention%20Data.csv) | The result: size of each cohort and % of it active in months 1 to 13 |
| [`plot_retention.py`](plot_retention.py) | Draws the heatmap above from the result |
| [`images/`](images) | The heatmap (light and dark versions) |

## How it works

### 1. Load the data

`retail_data` gets the 8 columns as they are, and `COPY` loads the CSV into it. The dates in the file are day-first but PostgreSQL reads them month-first by default, so the script runs `SET DateStyle = 'ISO, DMY'` before loading. Without it the load stops at the first date after the 12th of a month (`13/12/2010`).

### 2. Clean

| Step | Rows removed | Rows left |
| --- | ---: | ---: |
| Raw data | | 541,909 |
| No `CustomerID`: a customer we can't identify can't be followed over time | 135,080 | 406,829 |
| `Quantity <= 0`: cancellations, not purchases | 8,905 | 397,924 |
| `UnitPrice <= 0`: items given away for free | 40 | 397,884 |
| Exact duplicates (same values in all 8 columns): keep one copy | 5,192 | **392,692** |

Each step only counts the rows left after the previous one, which is why most of these numbers are smaller than the counts in the SQL comments. The cleaned data has **4,338 customers**, 18,532 invoices and 37 countries. It's saved as a table with `CREATE TABLE clean_data AS ...`, so the queries after it just read `clean_data`.

### 3. Top 3 customers by revenue in each country

Revenue is `SUM(Quantity * UnitPrice)` per customer, **minus their cancellations**, ranked within each country with `ROW_NUMBER() OVER (PARTITION BY Country ORDER BY total_revenue DESC)`. Subtracting cancellations matters: one UK customer ordered 80,995 paper craft birdies (£168,470) and cancelled the order 12 minutes later, and counting the order alone would put them 3rd in the UK.

The biggest customer overall is 14646 in the Netherlands, with £279,489. The next-biggest Dutch customer spent £1,531.

### 4. Cohort retention

Each step is a CTE:

1. **`first_purchase`**: a customer's cohort is the month of their first purchase, `MIN(DATE_TRUNC('month', InvoiceDate))`.
2. **`with_cohort`**: every purchase gets the customer's cohort and its own month.
3. **`cohort_index`**: the number of months between the two, counting the first month as 1. For a first purchase in Dec 2010 and another in Feb 2011: `(2011*12 + 2) - (2010*12 + 12) + 1 = 3`, so the second one is in month 3.
4. **`cohort_counts`**: the number of different customers in each cohort and month (`COUNT(DISTINCT CustomerID)`).
5. **Pivot**: one row per cohort, one column per month with `SUM(CASE WHEN cohort_index = k THEN users END)`, divided by the cohort size.

A customer counts as retained in month *k* if they bought anything in that month. They can skip a month and come back later, which is why the percentages can go up as well as down.

## Results

The numbers are in the heatmap at the top and in [`Cohort Retention Data.csv`](Cohort%20Retention%20Data.csv).

- **About 1 in 5 new customers order again the next month.** Month 2 retention for the Jan to Oct 2011 cohorts goes from 15.0% (Mar 2011) to 24.0% (Oct 2011), 20% on average.
- **Retention doesn't keep falling after that.** The 2011 cohorts stay between 15% and 37% and rise towards the end of the year.
- **November is the best month.** For 10 of the 11 cohorts that reach it, Nov 2011 is their best month after the first purchase (36.5% of the Jan 2011 cohort, for example), most likely gift shops and wholesalers stocking up for Christmas.
- **The Dec 2010 cohort (32% to 50% in every full month) can't be compared with the others.** The data starts on 1 Dec 2010, so everyone who bought that month lands in this cohort, including loyal customers who had been buying long before.
- **The last value of each row (marked \*) is too low.** It's December 2011, and the data stops on 9 Dec. The Feb 2011 cohort, for example, goes from 30.5% in November to 6.8% in December: that's missing data, not customers leaving.

## Run it

1. Install [PostgreSQL](https://www.postgresql.org/download/) and a client (pgAdmin or psql), and clone this repo.
2. In `Cohort.sql`, change the path in the `COPY` statement to where you saved `Retail_Data.csv`.
3. Run the script from top to bottom: open it in pgAdmin's Query Tool, or use psql (`-o` because the `SELECT *` queries print a lot of rows):

   ```bash
   psql -U postgres -d your_database -f Cohort.sql -o output.txt
   ```

   The last query returns the retention table. In pgAdmin, *Save results to file* exports it as a CSV.
4. If `COPY` fails with *permission denied*, the database server can't read the file (it needs superuser rights and access to your folder). Load it from psql with `\copy` instead, which reads the file on your side. Run this from the repo folder, in one psql session:

   ```sql
   SET DateStyle = 'ISO, DMY';
   \copy retail_data FROM 'Retail_Data.csv' DELIMITER ',' CSV HEADER
   ```

5. Optional, to redraw the heatmap after exporting the result: `pip install pandas matplotlib`, then `python plot_retention.py`.

Tested with PostgreSQL 16.

## SQL used

CTEs (`WITH`), window functions (`ROW_NUMBER() OVER (PARTITION BY ...)`), date functions (`DATE_TRUNC`, `EXTRACT`, `TO_CHAR`), pivoting with conditional aggregation (`SUM(CASE WHEN ...)`), `COUNT(DISTINCT ...)`, `JOIN ... USING`, `SELECT DISTINCT`, `CREATE TABLE ... AS` and `COPY`.
