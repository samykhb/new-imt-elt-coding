"""
TP3 — Shared pytest fixtures
=============================
Fixtures are fake DataFrames that mimic Bronze data.
They are automatically injected into tests by pytest when a test parameter
has the same name as a fixture function.

Example:
    # This fixture is defined here:
    @pytest.fixture
    def sample_products(): ...

    # Any test with "sample_products" as a parameter receives it automatically:
    def test_something(self, sample_products):
        # sample_products is the DataFrame returned by the fixture above
"""

import pytest
import pandas as pd


@pytest.fixture
def sample_products():
    """Fake products DataFrame mimicking Bronze data."""
    return pd.DataFrame({
        "product_id": [1, 2, 3],
        "display_name": ["Nike Air Max", "Adidas Ultraboost", "Jordan 1"],
        "brand": ["Nike", "Adidas", "Jordan"],
        "category": ["sneakers", "sneakers", "sneakers"],
        "price_usd": [149.99, 179.99, -10.00],  # one invalid price for testing
        "tags": ["running|casual", "running|boost", "retro|hype"],
        "is_active": [1, 1, 0],
        "is_hype_product": [0, 0, 1],
        "_internal_cost_usd": [50.0, 60.0, 70.0],
        "_supplier_id": ["SUP001", "SUP002", "SUP003"],
    })


@pytest.fixture
def sample_users():
    """Fake users DataFrame mimicking Bronze data."""
    return pd.DataFrame({
        "user_id": [1, 2],
        "email": [" Alice@Example.COM ", "bob@test.com"],
        "first_name": ["Alice", "Bob"],
        "last_name": ["Martin", "Smith"],
        "loyalty_tier": ["gold", None],
        "_hashed_password": ["abc123", "def456"],
        "_last_ip": ["1.2.3.4", "5.6.7.8"],
        "_device_fingerprint": ["fp1", "fp2"],
    })


@pytest.fixture
def sample_orders():
    """Fake orders DataFrame mimicking Bronze data."""
    return pd.DataFrame({
        "order_id": [1, 2, 3],
        "user_id": [1, 2, 1],
        "order_date": ["2026-02-10", "2026-02-11", "2026-02-12"],
        "status": ["delivered", "shipped", "invalid_status"],
        "total_usd": [149.99, 179.99, 50.0],
        "coupon_code": ["SAVE10", None, None],
        "_stripe_charge_id": ["ch_1", "ch_2", "ch_3"],
        "_fraud_score": [0.1, 0.2, 0.9],
    })

@pytest.fixture
def sample_order_line_items():
    """Fake order line items DataFrame mimicking Bronze warehouse/sales data."""
    return pd.DataFrame({
        "line_item_id": ["LI-00000001", "LI-00000002", "LI-00000003", "LI-00000004", "LI-00000005"],
        "order_id": ["ORD-100001", "ORD-100001", "ORD-100002", "ORD-100002", "ORD-100003"],
        "product_id": ["KE-10039", "KE-10212", "KE-10202", "KE-10138", "KE-10172"],
        "product_name": [
            "Nike Cortez Lucky Green",
            "Nike Heritage Waist Bag",
            "Puma Essentials Big Logo Hoodie",
            "Jordan Air Jordan 4 SE",
            "New Balance M1000 Chicago"
        ],
        "brand": ["Nike", "Nike", "Puma", "Jordan", "New Balance"],
        "category": ["sneakers", "accessories", "hoodies", "sneakers", "sneakers"],
        "sku": ["KE-10039", "KE-10212", "KE-10202", "KE-10138", "KE-10172"],
        "selected_size": ["36", "ONE SIZE", "XXL", "40.5", "41"],
        "colorway": ["Lucky Green", "Grey", "Light Bone/Olive", "Burgundy Crush", "Chicago"],
        "quantity": [-1, 1, 0, 4, 1],
        "unit_price_usd": [-100.45, 25.00, 61.59, 235.47, 204.43],
        "line_total_usd": [-100.45, 25.00, 61.59, 235.47, 204.43],
        "weight_grams": [749, 177, 182, 1066, 557],
        "_warehouse_id": ["WH-BRU", "WH-BRU", "WH-BRU", "WH-PAR", "WH-BRU"],
        "_internal_batch_code": ["BATCH-17251", "BATCH-87826", "BATCH-53887", "BATCH-31079", "BATCH-53588"],
        "_pick_slot": ["SLOT-C28", "SLOT-F06", "SLOT-F07", "SLOT-D02", "SLOT-B90"]
    })