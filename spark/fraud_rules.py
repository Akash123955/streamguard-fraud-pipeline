"""
StreamGuard — Fraud Detection Rules
=====================================
Defines all fraud detection logic as pure Python functions.
These are called by fraud_detector.py (the Spark Streaming job).

Keeping rules separate from the Spark job means:
  - Easy to unit-test without Spark (see tests/test_fraud_rules.py)
  - Rules can be updated without touching Spark infrastructure
  - Business analysts can read and review rules in isolation

Rules implemented:
  1. Velocity rule  — >5 transactions in 60 seconds per customer
  2. Amount rule    — single transaction > $1,000
  3. Geo rule       — same customer in 2+ countries within 10 minutes
  4. Merchant rule  — merchant_category is "unknown" or high-risk
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType, BooleanType

# ── Constants ──────────────────────────────────────────────────────────────────
HIGH_RISK_MERCHANT_CATEGORIES = {
    "unknown",
    "crypto_exchange",
    "wire_transfer",
}

HIGH_RISK_COUNTRIES = {
    "RU",   # Russia
    "NG",   # Nigeria
    "KP",   # North Korea
}

FRAUD_MERCHANT_NAMES = {
    "Dark Web Store",
    "CryptoFast Exchange",
    "QuickWire Transfer",
}

VELOCITY_WINDOW_SECONDS = 60        # Look back 60 seconds for velocity check
VELOCITY_THRESHOLD = 5              # >5 txns in window = fraud
GEO_WINDOW_MINUTES = 10            # Look back 10 minutes for geo check
HIGH_AMOUNT_THRESHOLD = 1000.0     # > $1,000 = high risk


def apply_amount_rule(df):
    """
    Amount Rule: Flag transactions over $1,000 as HIGH RISK.

    Why $1,000? Most everyday transactions are under this threshold.
    Large amounts combined with other signals (geo, merchant) = very likely fraud.

    Returns the DataFrame with two new columns:
      - amount_flag (bool): True if this transaction triggers the amount rule
      - amount_reason (str): Human-readable explanation
    """
    return df.withColumn(
        "amount_flag",
        F.col("amount") > HIGH_AMOUNT_THRESHOLD
    ).withColumn(
        "amount_reason",
        F.when(
            F.col("amount") > HIGH_AMOUNT_THRESHOLD,
            F.concat(
                F.lit("HIGH_AMOUNT: $"),
                F.col("amount").cast(StringType())
            )
        ).otherwise(F.lit(None).cast(StringType()))
    )


def apply_merchant_rule(df):
    """
    Merchant Rule: Flag transactions at high-risk merchant categories or known fraud merchants.

    High-risk categories like "unknown" and "crypto_exchange" are commonly
    associated with money laundering and card testing attacks.

    Returns the DataFrame with:
      - merchant_flag (bool): True if merchant is high risk
      - merchant_reason (str): Explanation
    """
    # Build Spark array literals for the sets
    high_risk_categories = list(HIGH_RISK_MERCHANT_CATEGORIES)
    fraud_merchant_names = list(FRAUD_MERCHANT_NAMES)

    return df.withColumn(
        "merchant_flag",
        F.col("merchant_category").isin(high_risk_categories) |
        F.col("merchant_name").isin(fraud_merchant_names)
    ).withColumn(
        "merchant_reason",
        F.when(
            F.col("merchant_name").isin(fraud_merchant_names),
            F.concat(F.lit("FRAUD_MERCHANT: "), F.col("merchant_name"))
        ).when(
            F.col("merchant_category").isin(high_risk_categories),
            F.concat(F.lit("HIGH_RISK_CATEGORY: "), F.col("merchant_category"))
        ).otherwise(F.lit(None).cast(StringType()))
    )


def apply_geo_country_rule(df):
    """
    Geo Rule (simplified, single-event): Flag transactions from high-risk countries.

    The full geo anomaly rule (same customer in 2 countries within 10 minutes)
    requires windowed aggregation and is handled in fraud_detector.py using
    Spark Structured Streaming watermarks.

    This simpler version flags individual transactions from known high-risk countries.

    Returns the DataFrame with:
      - geo_flag (bool): True if transaction originates from a high-risk country
      - geo_reason (str): Explanation
    """
    high_risk_countries = list(HIGH_RISK_COUNTRIES)

    return df.withColumn(
        "geo_flag",
        F.col("merchant_country").isin(high_risk_countries)
    ).withColumn(
        "geo_reason",
        F.when(
            F.col("merchant_country").isin(high_risk_countries),
            F.concat(F.lit("HIGH_RISK_COUNTRY: "), F.col("merchant_country"))
        ).otherwise(F.lit(None).cast(StringType()))
    )


def compute_fraud_score(df):
    """
    Compute a composite fraud score (0–100) and risk level.

    Scoring weights:
      - High amount:        +30 points
      - Suspicious merchant: +35 points
      - High-risk country:  +35 points

    Risk levels:
      - 0       → LOW      (no flags)
      - 1–30    → MEDIUM   (one minor signal)
      - 31–65   → HIGH     (one strong signal or two weak ones)
      - 66–100  → CRITICAL (multiple strong signals)

    The fraud_score and risk_level are the key outputs that get written
    to Kafka fraud-alerts and Snowflake.
    """
    df = df.withColumn(
        "fraud_score",
        (F.col("amount_flag").cast("int") * 30) +
        (F.col("merchant_flag").cast("int") * 35) +
        (F.col("geo_flag").cast("int") * 35)
    )

    df = df.withColumn(
        "risk_level",
        F.when(F.col("fraud_score") == 0, "LOW")
         .when(F.col("fraud_score") <= 30, "MEDIUM")
         .when(F.col("fraud_score") <= 65, "HIGH")
         .otherwise("CRITICAL")
    )

    # Combine all fraud reasons into one readable string
    df = df.withColumn(
        "fraud_reason",
        F.concat_ws(
            " | ",
            F.when(F.col("amount_reason").isNotNull(), F.col("amount_reason")),
            F.when(F.col("merchant_reason").isNotNull(), F.col("merchant_reason")),
            F.when(F.col("geo_reason").isNotNull(), F.col("geo_reason")),
        )
    )

    # Replace empty string with null when no flags triggered
    df = df.withColumn(
        "fraud_reason",
        F.when(F.col("fraud_reason") == "", F.lit(None).cast(StringType()))
         .otherwise(F.col("fraud_reason"))
    )

    return df
