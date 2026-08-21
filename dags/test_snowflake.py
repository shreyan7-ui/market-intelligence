from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


SNOWFLAKE_CONN_ID = "snowflake_market_intelligence"


def get_connection():
    hook = SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )
    return hook.get_conn()


def load_market_events():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        print("Connected to Snowflake")
        
        cursor.execute("""
            TRUNCATE TABLE MARKET_INTELLIGENCE.GOLD.MARKET_EVENTS
        """)

        cursor.execute("""
            COPY INTO MARKET_INTELLIGENCE.GOLD.MARKET_EVENTS
            FROM @MARKET_INTELLIGENCE.GOLD.MARKET_INTELLIGENCE_STAGE/market_events/
            FILE_FORMAT = (
                TYPE = PARQUET
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PATTERN = '.*\\.parquet'
            ON_ERROR = 'ABORT_STATEMENT'
        """)

        print("MARKET_EVENTS loaded successfully")

    finally:
        cursor.close()
        conn.close()


def load_stock_analytics():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        print("Loading Stock Analytics")
        
        cursor.execute("""
            TRUNCATE TABLE MARKET_INTELLIGENCE.GOLD.STOCK_ANALYTICS
        """)

        cursor.execute("""
            COPY INTO MARKET_INTELLIGENCE.GOLD.STOCK_ANALYTICS
            FROM @MARKET_INTELLIGENCE.GOLD.MARKET_INTELLIGENCE_STAGE/stock_analytics/
            FILE_FORMAT = (
                TYPE = PARQUET
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PATTERN = '.*\\.parquet'
            ON_ERROR = 'ABORT_STATEMENT'
        """)

        print("STOCK_ANALYTICS loaded successfully")

    finally:
        cursor.close()
        conn.close()


def load_news_analytics():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        print("Loading News Analytics")
        
        cursor.execute("""
            TRUNCATE TABLE MARKET_INTELLIGENCE.GOLD.NEWS_ANALYTICS
        """)

        cursor.execute("""
            COPY INTO MARKET_INTELLIGENCE.GOLD.NEWS_ANALYTICS
            FROM @MARKET_INTELLIGENCE.GOLD.MARKET_INTELLIGENCE_STAGE/news_analytics/
            FILE_FORMAT = (
                TYPE = PARQUET
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PATTERN = '.*\\.parquet'
            ON_ERROR = 'ABORT_STATEMENT'
        """)

        print("NEWS_ANALYTICS loaded successfully")

    finally:
        cursor.close()
        conn.close()
        


