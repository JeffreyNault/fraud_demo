# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Agent 3 — Competitors
# MAGIC **Domain:** Pricing, Promotions, Store Openings/Closures
# MAGIC
# MAGIC - Grain: SKU × Competitor Banner × Market × Week
# MAGIC - Source: Teradata via JDBC
# MAGIC - Refresh: Weekly — re-runs only on new data vintage
# MAGIC - Stale threshold: >7 days since last data vintage

# COMMAND ----------

# MAGIC %run ../shared/00_config

# COMMAND ----------

# MAGIC %run ../shared/00_utils

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timedelta

report = FlagReport(3, "Competitors", BUSINESS_DATE_STR)

# ─────────────────────────────────────────────
# 1. CONNECT TO TERADATA VIA JDBC
# ─────────────────────────────────────────────

jdbc_properties = {
    "user":     TERADATA_USER,
    "password": TERADATA_PASSWORD,
    "driver":   "com.teradata.jdbc.TeraDriver",
}

def read_teradata(query: str):
    return spark.read.jdbc(
        url=TERADATA_JDBC_URL,
        table=f"({query}) q",
        properties=jdbc_properties,
    )

# ─────────────────────────────────────────────
# 2. CHECK DATA VINTAGE
# ─────────────────────────────────────────────

vintage_query = f"""
SELECT MAX(week_ending_date) AS latest_week
FROM {TBL_COMPETITOR_PRICE}
WHERE category_cd NOT IN ('{EXCLUDED_CAT_SQL}')
"""

try:
    vintage_row = read_teradata(vintage_query).collect()[0]
    latest_week = vintage_row["latest_week"]
    days_since  = (datetime.today().date() - latest_week).days if latest_week else 999
except Exception as e:
    latest_week = None
    days_since  = 999
    report.add_data_quality_note(f"Teradata connection failed: {e}")

stale_threshold = THRESHOLDS["competitor_data_stale_days"]
is_stale = days_since > stale_threshold

if is_stale:
    report.add_flag(
        severity="WATCH",
        title="Competitor Data Stale",
        detail=f"Latest data vintage: {latest_week}. {days_since} days old — exceeds {stale_threshold}-day threshold.",
        forward_look="Verify Teradata feed and data pipeline for competitor pricing.",
    )
    agent_3_output = report.render()
    print(agent_3_output)
    write_flag_history(spark, TBL_FLAG_HISTORY, 3, report, BUSINESS_DATE_STR)
    dbutils.jobs.taskValues.set(key="output",       value=agent_3_output)
    dbutils.jobs.taskValues.set(key="status_color", value=report.status_color)
    dbutils.jobs.taskValues.set(key="data_vintage", value=str(latest_week))
    dbutils.notebook.exit(agent_3_output)

# ─────────────────────────────────────────────
# 3. CURRENT WEEK — PRICE INDEX BY SKU
# ─────────────────────────────────────────────

price_index_sql = f"""
SELECT
    p.sku_id,
    p.market_id,
    p.competitor_banner,
    p.our_price,
    p.competitor_price,
    p.price_index,
    p.category_cd,
    p.department_cd,
    p.week_ending_date
FROM {TBL_COMPETITOR_PRICE} p
WHERE p.week_ending_date = '{latest_week}'
  AND p.category_cd NOT IN ('{EXCLUDED_CAT_SQL}')
"""

# Prior week for WoW comparison
prior_week = (latest_week - timedelta(days=7)).strftime("%Y-%m-%d") if latest_week else None
prior_week_sql = f"""
SELECT sku_id, market_id, competitor_banner, price_index AS price_index_pw, category_cd
FROM {TBL_COMPETITOR_PRICE}
WHERE week_ending_date = '{prior_week}'
  AND category_cd NOT IN ('{EXCLUDED_CAT_SQL}')
"""

current_df = read_teradata(price_index_sql).alias("cur")
prior_df   = read_teradata(prior_week_sql).alias("prv")

price_df = (
    current_df
    .join(
        prior_df,
        on=["sku_id", "market_id", "competitor_banner", "category_cd"],
        how="left"
    )
    .withColumn("index_wow_delta", F.col("price_index") - F.col("price_index_pw"))
).cache()

# ─────────────────────────────────────────────
# 4. HIGH-VELOCITY SKU PRICING FLAGS  (>5% above competitor)
# ─────────────────────────────────────────────

# Join to product dimension to get velocity rank (from Databricks side)
velocity_sql = f"""
SELECT sku_id, velocity_rank
FROM {DIM_PRODUCT}
WHERE velocity_rank <= 100
"""
top_velocity = spark.sql(velocity_sql).toPandas()["sku_id"].tolist()

high_index_threshold = THRESHOLDS["price_index_flag_pct"]

high_index = (
    price_df
    .filter(
        (F.col("price_index") > (1 + high_index_threshold)) &
        (F.col("sku_id").isin(top_velocity))
    )
    .orderBy(F.col("price_index").desc())
    .limit(20)
).collect()

# ─────────────────────────────────────────────
# 5. CATEGORY-LEVEL INDEX SHIFTS WoW
# ─────────────────────────────────────────────

wow_threshold = THRESHOLDS["price_index_wow_flag"]

category_index_sql = f"""
SELECT
    category_cd,
    competitor_banner,
    ROUND(AVG(price_index), 4)           AS avg_index_cur,
    ROUND(AVG(price_index_pw), 4)        AS avg_index_pw,
    ROUND(AVG(price_index) - AVG(price_index_pw), 4) AS index_wow_delta,
    COUNT(DISTINCT sku_id)               AS sku_count
FROM ({price_df._jdf.showString(1, 1, False)})
GROUP BY category_cd, competitor_banner
HAVING ABS(AVG(price_index) - AVG(price_index_pw)) > {wow_threshold}
"""

# Use Spark directly since price_df is already a DataFrame
category_shifts = (
    price_df
    .groupBy("category_cd", "competitor_banner")
    .agg(
        F.round(F.avg("price_index"), 4).alias("avg_index_cur"),
        F.round(F.avg("price_index_pw"), 4).alias("avg_index_pw"),
        F.round(F.avg("price_index") - F.avg("price_index_pw"), 4).alias("index_wow_delta"),
        F.countDistinct("sku_id").alias("sku_count"),
    )
    .filter(F.abs(F.col("index_wow_delta")) > wow_threshold)
    .orderBy(F.col("index_wow_delta").desc())
).collect()

# ─────────────────────────────────────────────
# 6. BUILD FLAG REPORT
# ─────────────────────────────────────────────

if high_index:
    top_sku_lines = []
    for r in high_index[:10]:
        top_sku_lines.append(
            f"  SKU {r['sku_id']} ({r['category_cd']}) vs {r['competitor_banner']}: "
            f"index {r['price_index']:.3f} (our ${r['our_price']:.2f} / their ${r['competitor_price']:.2f})"
        )
    report.add_flag(
        severity="CRITICAL",
        title=f"Top-Velocity SKUs Priced >5% Above Competitor",
        detail=(
            f"{len(high_index)} high-velocity SKUs priced more than 5% above competitor. "
            f"Top examples:\n" + "\n".join(top_sku_lines)
        ),
        metric=f"{len(high_index)} SKUs flagged",
        forward_look=(
            "Pricing exposure on high-velocity items directly impacts basket conversion. "
            "Cross-reference Agent 2 for traffic/basket trends in affected markets."
        ),
    )

if category_shifts:
    shift_lines = []
    for r in category_shifts[:8]:
        direction = "worsened" if r["index_wow_delta"] > 0 else "improved"
        shift_lines.append(
            f"  {r['category_cd']} vs {r['competitor_banner']}: "
            f"index {r['avg_index_cur']:.3f} ({r['index_wow_delta']:+.3f} WoW, {direction})"
        )
    report.add_flag(
        severity="WATCH",
        title="Category-Level Price Index Shift WoW",
        detail=(
            f"{len(category_shifts)} category/competitor combinations with index shift >2% WoW:\n"
            + "\n".join(shift_lines)
        ),
        metric=f"{len(category_shifts)} category/banner shifts",
        forward_look="Monitor for customer traffic response over next 2 weeks.",
    )

if not high_index and not category_shifts:
    # No exceptions — will render GREEN
    pass

# ─────────────────────────────────────────────
# 7. OUTPUT
# ─────────────────────────────────────────────

agent_3_output = report.render()
print(agent_3_output)

write_flag_history(spark, TBL_FLAG_HISTORY, 3, report, BUSINESS_DATE_STR)

dbutils.jobs.taskValues.set(key="output",       value=agent_3_output)
dbutils.jobs.taskValues.set(key="status_color", value=report.status_color)
dbutils.jobs.taskValues.set(key="data_vintage", value=str(latest_week))
dbutils.jobs.taskValues.set(key="critical_count", value=report.critical_count)
dbutils.jobs.taskValues.set(key="watch_count",    value=report.watch_count)
dbutils.jobs.taskValues.set(key="win_count",      value=report.win_count)
