"""
StreamGuard — Unit Tests for Fraud Rules
==========================================
Tests the fraud detection logic WITHOUT needing Spark or Kafka running.
We use a local SparkSession in test mode.

Run with:
  pip install pytest pyspark
  pytest tests/test_fraud_rules.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
import sys
import os

# Add spark/ directory to path so we can import fraud_rules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark"))
from fraud_rules import (
    apply_amount_rule,
    apply_merchant_rule,
    apply_geo_country_rule,
    compute_fraud_score,
    HIGH_AMOUNT_THRESHOLD,
)


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing. Shared across all tests."""
    import sys
    # Tell Spark workers to use the same Python as the driver (our venv)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    session = (
        SparkSession.builder
        .appName("StreamGuard-Tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")  # Faster for small test data
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


TEST_SCHEMA = StructType([
    StructField("transaction_id",    StringType(),  True),
    StructField("customer_id",       StringType(),  True),
    StructField("amount",            DoubleType(),  True),
    StructField("merchant_name",     StringType(),  True),
    StructField("merchant_category", StringType(),  True),
    StructField("merchant_country",  StringType(),  True),
    StructField("customer_lat",      DoubleType(),  True),
    StructField("customer_lon",      DoubleType(),  True),
    StructField("is_fraud",          BooleanType(), True),
    StructField("fraud_type",        StringType(),  True),
])


def make_transaction(spark, **overrides):
    """Helper: create a single-row DataFrame with default transaction values."""
    defaults = {
        "transaction_id": "txn-001",
        "customer_id": "CUST_00001",
        "amount": 50.0,
        "merchant_name": "Corner Grocery",
        "merchant_category": "grocery",
        "merchant_country": "US",
        "customer_lat": 42.36,
        "customer_lon": -71.06,
        "is_fraud": False,
        "fraud_type": None,
    }
    defaults.update(overrides)
    return spark.createDataFrame([Row(**defaults)], schema=TEST_SCHEMA)


# ── Amount Rule Tests ──────────────────────────────────────────────────────────

class TestAmountRule:
    def test_normal_amount_not_flagged(self, spark):
        df = make_transaction(spark, amount=99.99)
        result = apply_amount_rule(df).collect()[0]
        assert result["amount_flag"] is False
        assert result["amount_reason"] is None

    def test_exactly_threshold_not_flagged(self, spark):
        """Edge case: exactly $1,000 should NOT trigger (rule is >$1,000)."""
        df = make_transaction(spark, amount=1000.0)
        result = apply_amount_rule(df).collect()[0]
        assert result["amount_flag"] is False

    def test_high_amount_flagged(self, spark):
        df = make_transaction(spark, amount=1500.0)
        result = apply_amount_rule(df).collect()[0]
        assert result["amount_flag"] is True
        assert "HIGH_AMOUNT" in result["amount_reason"]

    def test_fraud_amount_flagged(self, spark):
        """Test the exact fraud signature from the producer ($1,000–$9,999)."""
        df = make_transaction(spark, amount=9999.0)
        result = apply_amount_rule(df).collect()[0]
        assert result["amount_flag"] is True


# ── Merchant Rule Tests ────────────────────────────────────────────────────────

class TestMerchantRule:
    def test_legitimate_merchant_not_flagged(self, spark):
        df = make_transaction(spark, merchant_name="Whole Foods", merchant_category="grocery")
        result = apply_merchant_rule(df).collect()[0]
        assert result["merchant_flag"] is False

    def test_unknown_category_flagged(self, spark):
        df = make_transaction(spark, merchant_category="unknown")
        result = apply_merchant_rule(df).collect()[0]
        assert result["merchant_flag"] is True
        assert "HIGH_RISK_CATEGORY" in result["merchant_reason"]

    def test_crypto_exchange_flagged(self, spark):
        df = make_transaction(spark, merchant_category="crypto_exchange")
        result = apply_merchant_rule(df).collect()[0]
        assert result["merchant_flag"] is True

    def test_fraud_merchant_name_flagged(self, spark):
        df = make_transaction(spark, merchant_name="Dark Web Store")
        result = apply_merchant_rule(df).collect()[0]
        assert result["merchant_flag"] is True
        assert "FRAUD_MERCHANT" in result["merchant_reason"]
        assert "Dark Web Store" in result["merchant_reason"]


# ── Geo Rule Tests ─────────────────────────────────────────────────────────────

class TestGeoRule:
    def test_us_transaction_not_flagged(self, spark):
        df = make_transaction(spark, merchant_country="US")
        result = apply_geo_country_rule(df).collect()[0]
        assert result["geo_flag"] is False

    def test_russia_flagged(self, spark):
        df = make_transaction(spark, merchant_country="RU")
        result = apply_geo_country_rule(df).collect()[0]
        assert result["geo_flag"] is True
        assert "RU" in result["geo_reason"]

    def test_nigeria_flagged(self, spark):
        df = make_transaction(spark, merchant_country="NG")
        result = apply_geo_country_rule(df).collect()[0]
        assert result["geo_flag"] is True

    def test_uk_not_flagged(self, spark):
        df = make_transaction(spark, merchant_country="GB")
        result = apply_geo_country_rule(df).collect()[0]
        assert result["geo_flag"] is False


# ── Fraud Score Tests ──────────────────────────────────────────────────────────

class TestFraudScore:
    def _apply_all_rules(self, spark, **overrides):
        """Helper: apply all rules and compute score."""
        df = make_transaction(spark, **overrides)
        df = apply_amount_rule(df)
        df = apply_merchant_rule(df)
        df = apply_geo_country_rule(df)
        return compute_fraud_score(df).collect()[0]

    def test_clean_transaction_is_low_risk(self, spark):
        result = self._apply_all_rules(
            spark, amount=50.0, merchant_category="grocery",
            merchant_country="US", merchant_name="Corner Grocery"
        )
        assert result["fraud_score"] == 0
        assert result["risk_level"] == "LOW"
        assert result["fraud_reason"] is None

    def test_high_amount_only_is_medium(self, spark):
        result = self._apply_all_rules(
            spark, amount=1500.0, merchant_category="grocery",
            merchant_country="US", merchant_name="Corner Grocery"
        )
        assert result["fraud_score"] == 30
        assert result["risk_level"] == "MEDIUM"

    def test_suspicious_merchant_is_high(self, spark):
        result = self._apply_all_rules(
            spark, amount=50.0, merchant_category="unknown",
            merchant_country="US", merchant_name="Unknown Co"
        )
        assert result["fraud_score"] == 35
        assert result["risk_level"] == "HIGH"

    def test_full_fraud_signature_is_critical(self, spark):
        """Dark Web Store, $9,999, from Russia = maximum fraud score."""
        result = self._apply_all_rules(
            spark, amount=9999.0, merchant_name="Dark Web Store",
            merchant_category="unknown", merchant_country="RU"
        )
        assert result["fraud_score"] == 100
        assert result["risk_level"] == "CRITICAL"
        # All three reasons should appear
        assert "HIGH_AMOUNT" in result["fraud_reason"]
        assert "FRAUD_MERCHANT" in result["fraud_reason"]
        assert "HIGH_RISK_COUNTRY" in result["fraud_reason"]

    def test_fraud_reason_combines_multiple_signals(self, spark):
        """When multiple rules fire, reasons are joined with ' | '."""
        result = self._apply_all_rules(
            spark, amount=1500.0, merchant_category="crypto_exchange",
            merchant_country="US", merchant_name="CryptoFast"
        )
        assert " | " in result["fraud_reason"]
