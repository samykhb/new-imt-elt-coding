"""
SOLUTION — Gold Layer (Business Aggregations)
"""

import pandas as pd
from sqlalchemy import text

from src.database import get_engine, SILVER_SCHEMA, GOLD_SCHEMA
from src.logger import get_logger  


# Module-level logger (TP3)
logger = get_logger(__name__)    


def _read_silver(table_name: str) -> pd.DataFrame:
    """Read a table from the Silver schema."""
    engine = get_engine()
    query = f"SELECT * FROM {SILVER_SCHEMA}.{table_name}"
    return pd.read_sql(query, engine)


def _create_gold_table(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    """Load a DataFrame into a table in the Gold schema."""
    engine = get_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        schema=GOLD_SCHEMA,
        if_exists=if_exists,
        index=False,
    )
    # Remplacé par le logger (TP3)
    logger.info(f"{GOLD_SCHEMA}.{table_name} — {len(df)} rows")


def _create_gold_view(view_name: str, sql: str):
    """Create a SQL view in the Gold schema."""
    engine = get_engine()
    full_name = f"{GOLD_SCHEMA}.{view_name}"
    with engine.connect() as conn:
        conn.execute(text(f"DROP VIEW IF EXISTS {full_name}"))
        conn.execute(text(f"CREATE VIEW {full_name} AS {sql}"))
        conn.commit()
    # Remplacé par le logger (TP3)
    logger.info(f"View {full_name} created")


def create_daily_revenue():
    """Create gold.daily_revenue — daily revenue."""
    try:
        logger.info("Gold: daily_revenue")
        
        query = f"""
            SELECT
                DATE(o.order_date) AS order_date,
                COUNT(DISTINCT o.order_id) AS total_orders,
                ROUND(CAST(SUM(o.total_usd) AS numeric), 2) AS total_revenue,
                ROUND(CAST(AVG(o.total_usd) AS numeric), 2) AS avg_order_value,
                COALESCE(SUM(ol.quantity), 0) AS total_items
            FROM {SILVER_SCHEMA}.fct_orders o
            LEFT JOIN {SILVER_SCHEMA}.fct_order_lines ol ON o.order_id = ol.order_id
            WHERE o.status NOT IN ('cancelled', 'chargeback')
            GROUP BY DATE(o.order_date)
            ORDER BY order_date
        """
        df = pd.read_sql(query, get_engine())
        _create_gold_table(df, "daily_revenue")
        
    except Exception as e:
        logger.error("Failed to create daily_revenue", exc_info=True)
        raise


def create_product_performance():
    """Create gold.product_performance — metrics per product."""
    try:
        logger.info("Gold: product_performance")
        
        query = f"""
            SELECT
                ol.product_id,
                p.display_name AS product_name,
                p.brand,
                p.category,
                SUM(ol.quantity) AS total_quantity_sold,
                ROUND(CAST(SUM(ol.line_total_usd) AS numeric), 2) AS total_revenue,
                COUNT(DISTINCT ol.order_id) AS num_orders,
                ROUND(CAST(AVG(ol.unit_price_usd) AS numeric), 2) AS avg_unit_price
            FROM {SILVER_SCHEMA}.fct_order_lines ol
            INNER JOIN {SILVER_SCHEMA}.dim_products p ON ol.product_id = p.product_id
            INNER JOIN {SILVER_SCHEMA}.fct_orders o ON ol.order_id = o.order_id
            WHERE o.status NOT IN ('cancelled', 'chargeback')
            GROUP BY ol.product_id, p.display_name, p.brand, p.category
            ORDER BY total_revenue DESC
        """
        df = pd.read_sql(query, get_engine())
        _create_gold_table(df, "product_performance")
        
    except Exception as e:
        logger.error("Failed to create product_performance", exc_info=True)
        raise


def create_customer_ltv():
    """Create gold.customer_ltv — Lifetime Value per customer."""
    try:
        logger.info("Gold: customer_ltv")
        
        query = f"""
            SELECT
                u.user_id,
                u.email,
                u.first_name,
                u.last_name,
                u.loyalty_tier,
                COUNT(o.order_id) AS total_orders,
                ROUND(CAST(SUM(o.total_usd) AS numeric), 2) AS total_spent,
                ROUND(CAST(AVG(o.total_usd) AS numeric), 2) AS avg_order_value,
                MIN(o.order_date) AS first_order_date,
                MAo.order_date) AS last_order_date,
                EXTRACT(DAY FROM MAo.order_date) - MIN(o.order_date))::int AS days_as_customer
            FROM {SILVER_SCHEMA}.dim_users u
            INNER JOIN {SILVER_SCHEMA}.fct_orders o ON u.user_id = o.user_id
            WHERE o.status NOT IN ('cancelled', 'chargeback')
            GROUP BY u.user_id, u.email, u.first_name, u.last_name, u.loyalty_tier
            HAVING COUNT(o.order_id) > 0
            ORDER BY total_spent DESC
        """
        df = pd.read_sql(query, get_engine())
        _create_gold_table(df, "customer_ltv")
        
    except Exception as e:
        logger.error("Failed to create customer_ltv", exc_info=True)
        raise


def create_gold_layer():
    """Create all tables/views for the Gold layer."""
    logger.info(f"Starting Gold layer creation ({GOLD_SCHEMA})")
    
    # Appel des 3 fonctions
    create_daily_revenue()
    create_product_performance()
    create_customer_ltv()
    
    logger.info(f"Gold layer created successfully in {GOLD_SCHEMA}")


if __name__ == "__main__":
    create_gold_layer()