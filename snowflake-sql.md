# Snowflake SQL Guide

This guide contains the SQL commands used to inspect, load, validate, and troubleshoot the Gold layer in Snowflake.

---

## 1. Show Tables in Gold Schema

```sql
SHOW TABLES IN SCHEMA MARKET_INTELLIGENCE.GOLD;
```

---

## 2. Delete All Data from Gold Tables

Use `TRUNCATE TABLE` when you want to remove all rows while keeping the table structure.

```sql
TRUNCATE TABLE MARKET_INTELLIGENCE.GOLD.MARKET_EVENTS;

TRUNCATE TABLE MARKET_INTELLIGENCE.GOLD.STOCK_ANALYTICS;

TRUNCATE TABLE MARKET_INTELLIGENCE.GOLD.NEWS_ANALYTICS;
```

---

## 3. Show Stages and Tables

Show stages available in the Gold schema:

```sql
SHOW STAGES IN SCHEMA MARKET_INTELLIGENCE.GOLD;
```

Show tables available in the Gold schema:

```sql
SHOW TABLES IN SCHEMA MARKET_INTELLIGENCE.GOLD;
```

---

## 4. Check Files in the Snowflake Stage

List Parquet files available under the Market Events stage path:

```sql
LIST @MARKET_INTELLIGENCE.GOLD.MARKET_INTELLIGENCE_STAGE/market_events/;
```

---

# 5. Load Market Events from S3 into Snowflake

Copy the Market Events Parquet files from the external stage into the Snowflake table:

```sql
COPY INTO MARKET_INTELLIGENCE.GOLD.MARKET_EVENTS
FROM @MARKET_INTELLIGENCE.GOLD.MARKET_INTELLIGENCE_STAGE/market_events/
FILE_FORMAT = (
    TYPE = PARQUET
)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet'
ON_ERROR = 'ABORT_STATEMENT';
```

### Verify Market Events Row Count

```sql
SELECT COUNT(*) AS count_events
FROM MARKET_INTELLIGENCE.GOLD.MARKET_EVENTS;
```

---

# 6. Load News Analytics from S3 into Snowflake

Copy the News Analytics Parquet files into the Snowflake table:

```sql
COPY INTO MARKET_INTELLIGENCE.GOLD.NEWS_ANALYTICS
FROM @MARKET_INTELLIGENCE.GOLD.MARKET_INTELLIGENCE_STAGE/news_analytics/
FILE_FORMAT = (
    TYPE = PARQUET
)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet'
ON_ERROR = 'ABORT_STATEMENT';
```

### Verify News Analytics Row Count

```sql
SELECT COUNT(*)
FROM MARKET_INTELLIGENCE.GOLD.NEWS_ANALYTICS;
```

---

# 7. Load Stock Analytics from S3 into Snowflake

Copy the Stock Analytics Parquet files into the Snowflake table:

```sql
COPY INTO MARKET_INTELLIGENCE.GOLD.STOCK_ANALYTICS
FROM @MARKET_INTELLIGENCE.GOLD.MARKET_INTELLIGENCE_STAGE/stock_analytics/
FILE_FORMAT = (
    TYPE = PARQUET
)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet'
ON_ERROR = 'ABORT_STATEMENT';
```

### Verify Stock Analytics Row Count

```sql
SELECT COUNT(*)
FROM MARKET_INTELLIGENCE.GOLD.STOCK_ANALYTICS;
```

---

# 8. Count Rows Across All Gold Tables

Use this query to quickly verify the number of rows loaded into each Gold table:

```sql
SELECT 'MARKET_EVENTS' AS table_name, COUNT(*) AS row_count
FROM MARKET_INTELLIGENCE.GOLD.MARKET_EVENTS

UNION ALL

SELECT 'NEWS_ANALYTICS', COUNT(*)
FROM MARKET_INTELLIGENCE.GOLD.NEWS_ANALYTICS

UNION ALL

SELECT 'STOCK_ANALYTICS', COUNT(*)
FROM MARKET_INTELLIGENCE.GOLD.STOCK_ANALYTICS;
```

---

# 9. Check Table Columns

Describe the structure of each Gold table:

```sql
DESC TABLE MARKET_INTELLIGENCE.GOLD.MARKET_EVENTS;

DESC TABLE MARKET_INTELLIGENCE.GOLD.NEWS_ANALYTICS;

DESC TABLE MARKET_INTELLIGENCE.GOLD.STOCK_ANALYTICS;
```

---

# 10. Preview Gold Table Data

Preview Market Events:

```sql
SELECT *
FROM MARKET_INTELLIGENCE.GOLD.MARKET_EVENTS
LIMIT 20;
```

Preview News Analytics:

```sql
SELECT *
FROM MARKET_INTELLIGENCE.GOLD.NEWS_ANALYTICS
LIMIT 20;
```

Preview Stock Analytics:

```sql
SELECT *
FROM MARKET_INTELLIGENCE.GOLD.STOCK_ANALYTICS
LIMIT 20;
```

---

# 11. Verify Duplicates in Market Events

## Check Duplicate Event Groups

Check for duplicate combinations of:

- `TICKER`
- `EVENT_TIME`
- `NEWS_ID`

```sql
SELECT
    TICKER,
    EVENT_TIME,
    NEWS_ID,
    COUNT(*) AS CNT
FROM MARKET_INTELLIGENCE.GOLD.MARKET_EVENTS
GROUP BY TICKER, EVENT_TIME, NEWS_ID
HAVING COUNT(*) > 1
ORDER BY CNT DESC;
```

If this query returns no rows, there are no duplicate groups based on these three columns.

---

## Compare Total Rows vs Unique News IDs

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT NEWS_ID) AS unique_news_ids
FROM MARKET_INTELLIGENCE.GOLD.MARKET_EVENTS;
```

This helps identify whether the same `NEWS_ID` occurs multiple times.

---

## Count Rows by Ticker

```sql
SELECT
    TICKER,
    COUNT(*) AS ROW_COUNT
FROM MARKET_INTELLIGENCE.GOLD.MARKET_EVENTS
GROUP BY TICKER
ORDER BY ROW_COUNT DESC;
```

This helps verify that data exists for the expected stocks.

---

## Calculate Duplicate Event Rows

Calculate duplicates using the combination:

`TICKER + EVENT_TIME + NEWS_ID`

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT TICKER || '|' || EVENT_TIME || '|' || NEWS_ID) AS unique_events,
    COUNT(*) - COUNT(
        DISTINCT TICKER || '|' || EVENT_TIME || '|' || NEWS_ID
    ) AS duplicate_rows
FROM MARKET_INTELLIGENCE.GOLD.MARKET_EVENTS;
```

---

## Summarize Duplicate Groups

```sql
SELECT
    MAX(CNT) AS MAX_DUPLICATES,
    SUM(CNT - 1) AS DUPLICATE_ROWS,
    COUNT(*) AS DUPLICATE_GROUPS
FROM (
    SELECT
        TICKER,
        EVENT_TIME,
        NEWS_ID,
        COUNT(*) AS CNT
    FROM MARKET_INTELLIGENCE.GOLD.MARKET_EVENTS
    GROUP BY TICKER, EVENT_TIME, NEWS_ID
    HAVING COUNT(*) > 1
);
```

This provides three useful metrics:

- `MAX_DUPLICATES` → largest number of occurrences for a single event
- `DUPLICATE_ROWS` → total excess duplicate rows
- `DUPLICATE_GROUPS` → number of duplicated event groups

---

# 12. Snowflake Credentials / Connection Check

Check the current Snowflake account and region:

```sql
SELECT CURRENT_ACCOUNT(), CURRENT_REGION();
```

Show users matching a specific name:

```sql
SHOW USERS LIKE '';
```

### Change User Password

```sql
ALTER USER SHREYAN7 SET PASSWORD = '';
```

> **Security Warning:** Never commit real Snowflake passwords, access keys, tokens, or other credentials to GitHub. Keep credentials in environment variables, Airflow Connections, or another secrets-management mechanism.

---

# Quick Snowflake Verification Flow

Use this sequence when verifying that the Gold layer has been successfully loaded:

```text
Check Schema
     ↓
SHOW TABLES
     ↓
Check Stage
     ↓
LIST @stage/path
     ↓
COPY INTO Gold Tables
     ↓
Check Row Counts
     ↓
DESC TABLE
     ↓
Preview Data
     ↓
Check Duplicates
     ↓
Verify Final Gold Layer
```
