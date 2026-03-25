"""
SOLUTION — Transform (Silver Layer)
"""

import pandas as pd
from sqlalchemy import text

from src.database import get_engine, BRONZE_SCHEMA, SILVER_SCHEMA
from src.logger import get_logger  


# Module-level logger
logger = get_logger(__name__)      


def _read_bronze(table_name: str) -> pd.DataFrame:
    """Read a table from the Bronze schema via SQL."""
    engine = get_engine()
    query = f"SELECT * FROM {BRONZE_SCHEMA}.{table_name}"
    return pd.read_sql(query, engine)


def _drop_internal_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove all columns whose name starts with '_'."""
    internal_cols = [col for col in df.columns if col.startswith("_")]
    df = df.drop(columns=internal_cols)
    
    # Remplacé par le logger (TP3)
    if internal_cols:
        logger.info(f"{len(internal_cols)} internal columns removed: {internal_cols}")
    else:
        logger.info("No internal columns to remove")
    
    return df


def _load_to_silver(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    """Load a DataFrame into a table in the Silver schema."""
    engine = get_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        schema=SILVER_SCHEMA,
        if_exists=if_exists,
        index=False,
    )
    # Remplacé par le logger (TP3)
    logger.info(f"{SILVER_SCHEMA}.{table_name} — {len(df)} rows loaded")


def transform_products() -> pd.DataFrame:
    """Transform bronze.products → silver.dim_products."""
    try:
        logger.info("Transform: products → dim_products")
        
        df = _read_bronze("products")
        
        # Step 1 — Drop internal columns
        df = _drop_internal_columns(df)
        
        # Step 2 — Normalize the 'tags' column
        if "tags" in df.columns:
            df["tags"] = df["tags"].str.replace("|", ", ", regex=False)
        
        # Step 3 — Validate price_usd (remove rows where price <= 0)
        invalid_prices = df[df["price_usd"] <= 0]
        if len(invalid_prices) > 0:
            logger.warning(f"{len(invalid_prices)} products with price <= 0 (removed)")
        df = df[df["price_usd"] > 0]
        
        # Step 4 — Convert boolean columns
        for col in ["is_active", "is_hype_product"]:
            if col in df.columns:
                df[col] = df[col].astype(bool)
        
        # Step 5 — Load into Silver
        _load_to_silver(df, "dim_products")
        return df
        
    except Exception as e:
        logger.error("Failed to transform products", exc_info=True)
        raise


def transform_users() -> pd.DataFrame:
    """Transform bronze.users → silver.dim_users."""
    try:
        logger.info("Transform: users → dim_users")
        
        df = _read_bronze("users")
        
        # Step 1 — Drop internal columns (including PII)
        df = _drop_internal_columns(df)
        
        # Step 2 — Replace NULL loyalty_tier with 'none'
        df["loyalty_tier"] = df["loyalty_tier"].fillna("none")
        
        # Step 3 — Normalize emails (lowercase + strip)
        df["email"] = df["email"].str.lower().str.strip()
        
        # Step 4 — Load into Silver
        _load_to_silver(df, "dim_users")
        return df
        
    except Exception as e:
        logger.error("Failed to transform users", exc_info=True)
        raise


def transform_orders() -> pd.DataFrame:
    """Transform bronze.orders → silver.fct_orders."""
    try:
        logger.info("Transform: orders → fct_orders")
        
        df = _read_bronze("orders")
        
        # Step 1 — Drop internal columns
        df = _drop_internal_columns(df)
        
        # Step 2 — Validate statuses
        VALID_STATUSES = {"delivered", "shipped", "processing", "returned", "cancelled", "chargeback"}
        invalid = df[~df["status"].isin(VALID_STATUSES)]
        if len(invalid) > 0:
            logger.warning(f"{len(invalid)} orders with invalid status (removed)")
            df = df[df["status"].isin(VALID_STATUSES)]
        
        # Step 3 — Convert order_date to datetime
        df["order_date"] = pd.to_datetime(df["order_date"])
        
        # Step 4 — Replace NULL coupon_code with empty string
        df["coupon_code"] = df["coupon_code"].fillna("")
        
        # Step 5 — Load into Silver
        _load_to_silver(df, "fct_orders")
        return df
        
    except Exception as e:
        logger.error("Failed to transform orders", exc_info=True)
        raise


def transform_order_line_items() -> pd.DataFrame:
    """Transform bronze.order_line_items → silver.fct_order_lines."""
    try:
        logger.info("Transform: order_line_items → fct_order_lines")
        
        df = _read_bronze("order_line_items")
        
        # Step 1 — Drop internal columns
        df = _drop_internal_columns(df)
        
        # Step 2 — Validate quantity > 0
        invalid_qty = df[df["quantity"] <= 0]
        if len(invalid_qty) > 0:
            logger.warning(f"{len(invalid_qty)} rows with quantity <= 0 (removed)")
        df = df[df["quantity"] > 0]
        
        # Step 3 — Verify line_total_usd calculation
        df["_check"] = abs(df["line_total_usd"] - df["unit_price_usd"] * df["quantity"])
        bad_rows = df[df["_check"] > 0.01]
        if len(bad_rows) > 0:
            logger.info(f"{len(bad_rows)} rows with inconsistent line_total")
        df = df.drop(columns=["_check"])
        
        # Step 4 — Load into Silver
        _load_to_silver(df, "fct_order_lines")
        return df
        
    except Exception as e:
        logger.error("Failed to transform order_line_items", exc_info=True)
        raise


def transform_all() -> dict[str, pd.DataFrame]:
    """Run the complete transformation from Bronze → Silver."""
    logger.info(f"Starting transformation to Silver layer ({SILVER_SCHEMA})")
    
    results = {}
    
    results["dim_products"] = transform_products()
    results["dim_users"] = transform_users()
    results["fct_orders"] = transform_orders()
    results["fct_order_lines"] = transform_order_line_items()
    
    logger.info(f"Transformation complete — {len(results)} tables in {SILVER_SCHEMA}")
    return results


if __name__ == "__main__":
    transform_all()