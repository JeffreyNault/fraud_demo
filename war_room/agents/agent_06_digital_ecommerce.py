# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Agent 6 — Digital & E-Commerce
# MAGIC **Domain:** Site Traffic, Digital Revenue, BOPIS, Fulfillment SLA
# MAGIC
# MAGIC - Grain: Day (company-wide digital)
# MAGIC - Primary comparison: YoY
# MAGIC - Digital revenue is a component of total company comp — reconciles with Agent 1
# MAGIC - Conversion rate and cart abandonment deferred to v2

# COMMAND ----------

# MAGIC %run ../shared/00_config

# COMMAND ----------

# MAGIC %run ../shared/00_utils

# COMMAND ----------

from pyspark.sql import functions as F

report = FlagReport(6, "Digital & E-Commerce", BUSINESS_DATE_STR)

# ─────────────────────────────────────────────
# 1. DIGITAL GOLD TABLE — YoY
# ─────────────────────────────────────────────

digital_sql = f"""
SELECT
    business_date,
    sessions,
    unique_visitors,
    digital_revenue_ty,
    digital_revenue_ly,
    bopis_orders_ty,
    bopis_orders_ly,
    curbside_orders_ty,
    curbside_orders_ly,
    fulfillment_sla_pct,
    on_time_rate,
    ROUND((digital_revenue_ty - digital_revenue_ly) / NULLIF(digital_revenue_ly, 0), 4)     AS digital_rev_yoy_pct,
    ROUND((bopis_orders_ty    - bopis_orders_ly)    / NULLIF(bopis_orders_ly,    0), 4)     AS bopis_yoy_pct,
    ROUND((curbside_orders_ty - curbside_orders_ly) / NULLIF(curbside_orders_ly, 0), 4)     AS curbside_yoy_pct
FROM {TBL_DIGITAL_GOLD}
WHERE business_date = '{BUSINESS_DATE_STR}'
"""

digital_rows = spark.sql(digital_sql).collect()

if not digital_rows:
    report.add_data_quality_note(f"No digital data found for {BUSINESS_DATE_STR}.")
    agent_6_output = report.render()
    print(agent_6_output)
    dbutils.jobs.taskValues.set(key="output",       value=agent_6_output)
    dbutils.jobs.taskValues.set(key="status_color", value="YELLOW")
    dbutils.notebook.exit(agent_6_output)

row = digital_rows[0]

digital_rev_yoy = row["digital_rev_yoy_pct"] or 0.0
bopis_yoy       = row["bopis_yoy_pct"]        or 0.0
curbside_yoy    = row["curbside_yoy_pct"]     or 0.0
sla_pct         = row["fulfillment_sla_pct"]  or 1.0
on_time         = row["on_time_rate"]         or 1.0
rev_ty          = row["digital_revenue_ty"]   or 0
rev_ly          = row["digital_revenue_ly"]   or 0
rev_delta       = rev_ty - rev_ly

# ─────────────────────────────────────────────
# 2. FULFILLMENT SLA — RAW ORDER LEVEL DETAIL
# ─────────────────────────────────────────────

fulfillment_sql = f"""
SELECT
    fulfillment_type,
    COUNT(*)                                                         AS total_orders,
    SUM(CASE WHEN fulfilled_on_time = 1 THEN 1 ELSE 0 END)          AS on_time_orders,
    ROUND(AVG(CASE WHEN fulfilled_on_time = 1 THEN 1.0 ELSE 0 END), 4) AS sla_rate,
    AVG(DATEDIFF(actual_fulfillment_ts, promised_fulfillment_ts))    AS avg_delay_hrs
FROM {TBL_FULFILLMENT}
WHERE DATE(order_date) = '{BUSINESS_DATE_STR}'
  AND fulfillment_type IN ('BOPIS', 'CURBSIDE', 'DELIVERY')
GROUP BY fulfillment_type
"""

fulfillment_detail = spark.sql(fulfillment_sql).collect()

# ─────────────────────────────────────────────
# 3. 7-DAY ROLLING DIGITAL REVENUE TREND
# ─────────────────────────────────────────────

rolling7_sql = f"""
SELECT
    ROUND(AVG((digital_revenue_ty - digital_revenue_ly) / NULLIF(digital_revenue_ly, 0)), 4) AS rev_yoy_7d_avg
FROM {TBL_DIGITAL_GOLD}
WHERE business_date BETWEEN DATE_ADD('{BUSINESS_DATE_STR}', -7) AND DATE_ADD('{BUSINESS_DATE_STR}', -1)
"""

rolling7 = spark.sql(rolling7_sql).collect()[0]["rev_yoy_7d_avg"] or 0.0

# ─────────────────────────────────────────────
# 4. BUILD FLAG REPORT
# ─────────────────────────────────────────────

rev_threshold   = THRESHOLDS["digital_revenue_flag_pct"]
sla_threshold   = THRESHOLDS["fulfillment_sla_threshold"]
bopis_threshold = THRESHOLDS["bopis_swing_pct"]

# Digital Revenue YoY
if abs(digital_rev_yoy) > rev_threshold:
    direction = "below" if digital_rev_yoy < 0 else "above"
    severity  = "CRITICAL" if abs(digital_rev_yoy) > rev_threshold * 2 else "WATCH"
    report.add_flag(
        severity=severity,
        title=f"Digital Revenue {direction.upper()} LY by >{rev_threshold:.0%}",
        detail=(
            f"Digital revenue {digital_rev_yoy:+.1%} vs LY "
            f"(${rev_ty:,.0f} vs ${rev_ly:,.0f}). "
            f"7-day rolling trend: {rolling7:+.1%}."
        ),
        metric=f"YoY: {digital_rev_yoy:+.1%}  |  7d avg: {rolling7:+.1%}",
        impact=f"${rev_delta:+,.0f} vs LY",
        forward_look=(
            "Digital revenue is a component of total company comp. "
            "Commander must reconcile with Agent 1 store comp for total company picture."
        ),
    )
elif digital_rev_yoy > rev_threshold * 2:
    report.add_flag(
        severity="WIN",
        title="Digital Revenue Running Well Above LY",
        detail=f"Digital revenue {digital_rev_yoy:+.1%} vs LY (${rev_delta:+,.0f}).",
        impact=f"${rev_delta:+,.0f} vs LY",
    )

# Fulfillment SLA
if sla_pct < sla_threshold:
    report.add_flag(
        severity="CRITICAL",
        title=f"Fulfillment SLA Below {sla_threshold:.0%} Threshold",
        detail=f"Company-wide fulfillment SLA: {sla_pct:.1%}. On-time rate: {on_time:.1%}.",
        metric=f"SLA: {sla_pct:.1%}  |  On-time: {on_time:.1%}",
        forward_look="SLA failures drive cancellations and satisfaction decline. Cross-reference Agent 8 for supply chain root cause.",
    )

# Fulfillment by type
for frow in fulfillment_detail:
    ftype   = frow["fulfillment_type"]
    sla_r   = frow["sla_rate"] or 1.0
    delay   = frow["avg_delay_hrs"] or 0
    if sla_r < sla_threshold:
        report.add_flag(
            severity="WATCH",
            title=f"{ftype} Fulfillment SLA Breach",
            detail=f"{ftype}: {sla_r:.1%} SLA ({frow['on_time_orders']:,}/{frow['total_orders']:,} on time). Avg delay: {delay:.1f} hrs.",
            metric=f"SLA: {sla_r:.1%}",
        )

# BOPIS / Curbside swings
if abs(bopis_yoy) > bopis_threshold:
    direction = "down" if bopis_yoy < 0 else "up"
    report.add_flag(
        severity="WATCH",
        title=f"BOPIS Orders {direction.upper()} >10% vs LY",
        detail=(
            f"BOPIS orders {bopis_yoy:+.1%} vs LY "
            f"({row['bopis_orders_ty']:,} vs {row['bopis_orders_ly']:,} orders)."
        ),
        metric=f"BOPIS YoY: {bopis_yoy:+.1%}",
        forward_look="BOPIS shift may indicate store experience change or app friction. Monitor fulfillment SLA.",
    )

if abs(curbside_yoy) > bopis_threshold:
    direction = "down" if curbside_yoy < 0 else "up"
    report.add_flag(
        severity="WATCH",
        title=f"Curbside Orders {direction.upper()} >10% vs LY",
        detail=(
            f"Curbside {curbside_yoy:+.1%} vs LY "
            f"({row['curbside_orders_ty']:,} vs {row['curbside_orders_ly']:,} orders)."
        ),
        metric=f"Curbside YoY: {curbside_yoy:+.1%}",
    )

# ─────────────────────────────────────────────
# 5. OUTPUT
# ─────────────────────────────────────────────

agent_6_output = report.render()
print(agent_6_output)

write_flag_history(spark, TBL_FLAG_HISTORY, 6, report, BUSINESS_DATE_STR)

dbutils.jobs.taskValues.set(key="output",          value=agent_6_output)
dbutils.jobs.taskValues.set(key="status_color",    value=report.status_color)
dbutils.jobs.taskValues.set(key="critical_count",  value=report.critical_count)
dbutils.jobs.taskValues.set(key="watch_count",     value=report.watch_count)
dbutils.jobs.taskValues.set(key="win_count",       value=report.win_count)
dbutils.jobs.taskValues.set(key="digital_rev_yoy", value=float(digital_rev_yoy))
dbutils.jobs.taskValues.set(key="rev_delta",       value=float(rev_delta))
