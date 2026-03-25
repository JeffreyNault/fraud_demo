# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Agent 1 — Store Operations
# MAGIC **Domain:** Traffic, Comp Sales, Shrink, Labor
# MAGIC
# MAGIC - Grain: Store × Day
# MAGIC - Primary: Gold/mart comp sales table (includes plan)
# MAGIC - Secondary: Loss prevention table (shrink)
# MAGIC - Exception threshold: ±2% vs plan or LY
# MAGIC - Excludes: Tobacco, Gift Cards; no cross-format benchmarking

# COMMAND ----------

# MAGIC %run ../shared/00_config

# COMMAND ----------

# MAGIC %run ../shared/00_utils

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

report = FlagReport(1, "Store Operations", BUSINESS_DATE_STR)

# ─────────────────────────────────────────────
# 1. COMP SALES vs LY AND PLAN
# ─────────────────────────────────────────────

comp_sql = f"""
SELECT
    c.store_id,
    s.store_name,
    s.format_cd,
    s.region_cd,
    s.district_cd,
    c.business_date,
    c.net_sales_actual,
    c.net_sales_ly,
    c.net_sales_plan,
    c.transaction_count,
    c.avg_transaction_value,
    -- YoY and plan variance
    ROUND((c.net_sales_actual - c.net_sales_ly)   / NULLIF(c.net_sales_ly,   0), 4) AS yoy_pct,
    ROUND((c.net_sales_actual - c.net_sales_plan) / NULLIF(c.net_sales_plan, 0), 4) AS plan_pct,
    -- Transaction count YoY (traffic proxy)
    ROUND((c.transaction_count - c.transaction_count_ly) / NULLIF(c.transaction_count_ly, 0), 4) AS txn_yoy_pct
FROM {TBL_COMP_SALES} c
JOIN {DIM_STORE} s ON c.store_id = s.store_id
WHERE c.business_date = '{BUSINESS_DATE_STR}'
  AND s.format_cd IS NOT NULL
  AND c.category_cd NOT IN ('{EXCLUDED_CAT_SQL}')
"""

comp_df = spark.sql(comp_sql).cache()

# Rolling 7-day average per store (for context, not exception threshold)
rolling_sql = f"""
SELECT
    c.store_id,
    s.format_cd,
    ROUND(AVG((c.net_sales_actual - c.net_sales_ly) / NULLIF(c.net_sales_ly, 0)), 4) AS yoy_pct_7d_avg
FROM {TBL_COMP_SALES} c
JOIN {DIM_STORE} s ON c.store_id = s.store_id
WHERE c.business_date BETWEEN DATE_ADD('{BUSINESS_DATE_STR}', -7) AND DATE_ADD('{BUSINESS_DATE_STR}', -1)
  AND c.category_cd NOT IN ('{EXCLUDED_CAT_SQL}')
GROUP BY c.store_id, s.format_cd
"""
rolling_df = spark.sql(rolling_sql).cache()

# Join rolling context
comp_with_rolling = comp_df.join(rolling_df, on=["store_id", "format_cd"], how="left")

threshold = THRESHOLDS["store_comp_flag_pct"]

flagged = comp_with_rolling.filter(
    (F.abs(F.col("yoy_pct"))  > threshold) |
    (F.abs(F.col("plan_pct")) > threshold)
)

flagged_rows = flagged.orderBy(F.col("yoy_pct").asc()).collect()

# ─────────────────────────────────────────────
# 2. LABOR — Overtime and Schedule Adherence
# ─────────────────────────────────────────────

labor_sql = f"""
SELECT
    c.store_id,
    s.format_cd,
    s.region_cd,
    c.labor_hrs_actual,
    c.labor_hrs_scheduled,
    c.overtime_hrs,
    ROUND(c.labor_hrs_actual / NULLIF(c.labor_hrs_scheduled, 0), 4) AS labor_utilization,
    ROUND(c.overtime_hrs / NULLIF(c.labor_hrs_scheduled, 0), 4)     AS ot_ratio
FROM {TBL_COMP_SALES} c
JOIN {DIM_STORE} s ON c.store_id = s.store_id
WHERE c.business_date = '{BUSINESS_DATE_STR}'
  AND c.overtime_hrs / NULLIF(c.labor_hrs_scheduled, 0) > 0.10   -- flag >10% OT
"""

labor_flagged = spark.sql(labor_sql).collect()

# ─────────────────────────────────────────────
# 3. SHRINK
# ─────────────────────────────────────────────

shrink_sql = f"""
SELECT
    lp.store_id,
    s.format_cd,
    s.region_cd,
    lp.shrink_dollars,
    lp.shrink_dollars_ly,
    ROUND((lp.shrink_dollars - lp.shrink_dollars_ly) / NULLIF(lp.shrink_dollars_ly, 0), 4) AS shrink_yoy_pct
FROM {TBL_LOSS_PREVENTION} lp
JOIN {DIM_STORE} s ON lp.store_id = s.store_id
WHERE lp.business_date = '{BUSINESS_DATE_STR}'
  AND (lp.shrink_dollars - lp.shrink_dollars_ly) / NULLIF(lp.shrink_dollars_ly, 0) > 0.10
"""

shrink_flagged = spark.sql(shrink_sql).collect()

# ─────────────────────────────────────────────
# 4. SUMMARISE FLAGS BY FORMAT (no cross-format benchmarking)
# ─────────────────────────────────────────────

format_summary = (
    comp_df
    .groupBy("format_cd")
    .agg(
        F.count("store_id").alias("store_count"),
        F.round(F.sum("net_sales_actual"), 0).alias("total_sales_actual"),
        F.round(F.sum("net_sales_ly"), 0).alias("total_sales_ly"),
        F.round(
            (F.sum("net_sales_actual") - F.sum("net_sales_ly")) / F.sum("net_sales_ly"),
            4
        ).alias("format_yoy_pct"),
        F.round(
            (F.sum("net_sales_actual") - F.sum("net_sales_plan")) / F.sum("net_sales_plan"),
            4
        ).alias("format_plan_pct"),
    )
    .collect()
)

# ─────────────────────────────────────────────
# 5. BUILD FLAG REPORT
# ─────────────────────────────────────────────

# Comp Sales — format-level summary flags
for row in format_summary:
    fmt      = row["format_cd"]
    yoy_pct  = row["format_yoy_pct"] or 0.0
    plan_pct = row["format_plan_pct"] or 0.0
    sales_act = row["total_sales_actual"] or 0
    sales_ly  = row["total_sales_ly"] or 0
    dollar_delta = sales_act - sales_ly

    if abs(yoy_pct) > threshold or abs(plan_pct) > threshold:
        severity = "CRITICAL" if abs(yoy_pct) > threshold * 2 else "WATCH"
        direction = "below" if yoy_pct < 0 else "above"
        report.add_flag(
            severity=severity,
            title=f"{fmt} Format Comp Sales {direction.upper()} LY",
            detail=(
                f"{fmt} format ran {yoy_pct:+.1%} vs LY and {plan_pct:+.1%} vs plan "
                f"across {row['store_count']} stores."
            ),
            metric=f"YoY: {yoy_pct:+.1%}  |  vs Plan: {plan_pct:+.1%}",
            impact=f"${dollar_delta:+,.0f} vs LY",
            forward_look=(
                "Store-level detail flagged separately. Cross-reference Agent 2 "
                "for traffic decomposition."
            ),
        )

# Store-level detail for material misses
missing_stores = [
    r for r in flagged_rows
    if abs(r["yoy_pct"] or 0) > threshold * 2   # surface worst offenders
][:10]   # cap at 10 stores in output

if missing_stores:
    store_lines = []
    for r in missing_stores:
        store_lines.append(
            f"  Store {r['store_id']} ({r['format_cd']}, {r['district_cd']}): "
            f"YoY {r['yoy_pct']:+.1%} | 7d avg {r['yoy_pct_7d_avg'] or 0:+.1%}"
        )
    report.add_flag(
        severity="CRITICAL",
        title="Worst-Performing Stores — Comp Miss Exceeds 4% vs LY",
        detail="Top underperforming stores by YoY comp variance:\n" + "\n".join(store_lines),
        metric=f"{len(missing_stores)} stores flagged",
        forward_look="Evaluate whether traffic or basket is driving each store's miss (Agent 2).",
    )

# Traffic (transaction count)
traffic_miss = [r for r in flagged_rows if (r["txn_yoy_pct"] or 0) < -threshold]
if traffic_miss:
    avg_txn_miss = sum(r["txn_yoy_pct"] or 0 for r in traffic_miss) / len(traffic_miss)
    report.add_flag(
        severity="WATCH",
        title="Transaction Count Declining vs LY",
        detail=(
            f"{len(traffic_miss)} stores showing transaction count below LY. "
            f"Average transaction YoY: {avg_txn_miss:+.1%}."
        ),
        metric=f"Avg txn YoY: {avg_txn_miss:+.1%} across {len(traffic_miss)} stores",
        forward_look="Cross-reference Agent 2 for household segment driver and Agent 5 for weather impact.",
    )

# Labor — overtime flags
if labor_flagged:
    report.add_flag(
        severity="WATCH",
        title="Overtime Rate Elevated — >10% of Scheduled Hours",
        detail=(
            f"{len(labor_flagged)} stores with overtime ratio >10% of scheduled hours."
        ),
        metric=f"{len(labor_flagged)} stores flagged",
        forward_look="Persistent OT signals understaffing or schedule plan gap.",
    )

# Shrink flags
if shrink_flagged:
    total_shrink_delta = sum(
        (r["shrink_dollars"] - r["shrink_dollars_ly"]) for r in shrink_flagged
    )
    report.add_flag(
        severity="WATCH",
        title="Shrink Elevated vs LY",
        detail=(
            f"{len(shrink_flagged)} stores with shrink >10% above LY. "
            f"Combined incremental shrink: ${total_shrink_delta:,.0f}."
        ),
        metric=f"${total_shrink_delta:,.0f} incremental shrink vs LY",
        impact=f"${total_shrink_delta:,.0f}",
    )

# ─────────────────────────────────────────────
# 6. OUTPUT
# ─────────────────────────────────────────────

agent_1_output = report.render()
print(agent_1_output)

# Persist flags to history
write_flag_history(spark, TBL_FLAG_HISTORY, 1, report, BUSINESS_DATE_STR)

# Publish for downstream Commander task
dbutils.jobs.taskValues.set(key="output",         value=agent_1_output)
dbutils.jobs.taskValues.set(key="status_color",   value=report.status_color)
dbutils.jobs.taskValues.set(key="critical_count", value=report.critical_count)
dbutils.jobs.taskValues.set(key="watch_count",    value=report.watch_count)
dbutils.jobs.taskValues.set(key="win_count",      value=report.win_count)
