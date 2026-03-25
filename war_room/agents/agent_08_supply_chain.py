# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Agent 8 — Supply Chain & Logistics
# MAGIC **Domain:** Inbound Freight, Vendor Fill Rate, Store Replenishment
# MAGIC
# MAGIC - Grain: Vendor × Item × Day (fill rate); PO × Day (freight); Store × Day (replenishment)
# MAGIC - Top priority: Vendor fill rate below threshold on ANY active promo item — always CRITICAL
# MAGIC - Comparison: vs prior 7-day average
# MAGIC - Forward look: promo items with fill rate risk in next 7 days

# COMMAND ----------

# MAGIC %run ../shared/00_config

# COMMAND ----------

# MAGIC %run ../shared/00_utils

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import timedelta

report = FlagReport(8, "Supply Chain & Logistics", BUSINESS_DATE_STR)

fill_rate_threshold = THRESHOLDS["vendor_fill_rate_threshold"]
replen_sla_hrs      = THRESHOLDS["replenishment_sla_hrs"]

# ─────────────────────────────────────────────
# 1. VENDOR FILL RATE — PROMO ITEMS (HIGHEST PRIORITY)
# ─────────────────────────────────────────────

promo_fill_sql = f"""
SELECT
    sc.vendor_id,
    sc.sku_id,
    p.product_desc,
    p.category_cd,
    sc.vendor_fill_rate_pct,
    sc.promo_flag,
    pc.promo_type,
    pc.promo_end_date,
    -- 7-day average for comparison
    hist.avg_fill_rate_7d
FROM {TBL_SUPPLY_CHAIN} sc
JOIN {DIM_PRODUCT}       p  ON sc.sku_id = p.sku_id
JOIN {TBL_PROMO_CALENDAR} pc ON sc.sku_id = pc.sku_id
    AND '{BUSINESS_DATE_STR}' BETWEEN pc.promo_start_date AND pc.promo_end_date
LEFT JOIN (
    SELECT vendor_id, sku_id,
           ROUND(AVG(vendor_fill_rate_pct), 4) AS avg_fill_rate_7d
    FROM {TBL_SUPPLY_CHAIN}
    WHERE business_date BETWEEN DATE_ADD('{BUSINESS_DATE_STR}', -7) AND DATE_ADD('{BUSINESS_DATE_STR}', -1)
    GROUP BY vendor_id, sku_id
) hist ON sc.vendor_id = hist.vendor_id AND sc.sku_id = hist.sku_id
WHERE sc.business_date = '{BUSINESS_DATE_STR}'
  AND sc.promo_flag = 1
  AND sc.vendor_fill_rate_pct < {fill_rate_threshold}
  AND p.category_cd NOT IN ('{EXCLUDED_CAT_SQL}')
ORDER BY sc.vendor_fill_rate_pct ASC
"""

promo_fill_flagged = spark.sql(promo_fill_sql).collect()

# ─────────────────────────────────────────────
# 2. ALL VENDOR FILL RATE — vs 7-day avg
# ─────────────────────────────────────────────

fill_rate_sql = f"""
SELECT
    sc.vendor_id,
    sc.sku_id,
    p.category_cd,
    sc.vendor_fill_rate_pct,
    sc.promo_flag,
    hist.avg_fill_rate_7d,
    (sc.vendor_fill_rate_pct - hist.avg_fill_rate_7d) AS fill_rate_delta
FROM {TBL_SUPPLY_CHAIN} sc
JOIN {DIM_PRODUCT} p ON sc.sku_id = p.sku_id
LEFT JOIN (
    SELECT vendor_id, sku_id,
           ROUND(AVG(vendor_fill_rate_pct), 4) AS avg_fill_rate_7d
    FROM {TBL_SUPPLY_CHAIN}
    WHERE business_date BETWEEN DATE_ADD('{BUSINESS_DATE_STR}', -7) AND DATE_ADD('{BUSINESS_DATE_STR}', -1)
    GROUP BY vendor_id, sku_id
) hist ON sc.vendor_id = hist.vendor_id AND sc.sku_id = hist.sku_id
WHERE sc.business_date = '{BUSINESS_DATE_STR}'
  AND sc.vendor_fill_rate_pct < {fill_rate_threshold}
  AND p.category_cd NOT IN ('{EXCLUDED_CAT_SQL}')
  AND sc.promo_flag = 0    -- non-promo (promo covered above)
ORDER BY sc.vendor_fill_rate_pct ASC
"""

fill_rate_flagged = spark.sql(fill_rate_sql).collect()

# ─────────────────────────────────────────────
# 3. INBOUND PO DELAYS
# ─────────────────────────────────────────────

po_delay_sql = f"""
SELECT
    po.po_id,
    po.vendor_id,
    po.sku_id,
    p.category_cd,
    po.po_expected_receipt_date,
    po.po_actual_receipt_date,
    po.promo_flag,
    DATEDIFF(COALESCE(po.po_actual_receipt_date, CURRENT_DATE()), po.po_expected_receipt_date) AS delay_days
FROM {TBL_WMS_PO} po
JOIN {DIM_PRODUCT} p ON po.sku_id = p.sku_id
WHERE po.po_expected_receipt_date <= '{BUSINESS_DATE_STR}'
  AND (po.po_actual_receipt_date IS NULL OR po.po_actual_receipt_date > po.po_expected_receipt_date)
  AND DATEDIFF(COALESCE(po.po_actual_receipt_date, CURRENT_DATE()), po.po_expected_receipt_date) > 1
  AND p.category_cd NOT IN ('{EXCLUDED_CAT_SQL}')
ORDER BY po.promo_flag DESC, delay_days DESC
"""

po_delays = spark.sql(po_delay_sql).collect()

# ─────────────────────────────────────────────
# 4. STORE REPLENISHMENT SLA
# ─────────────────────────────────────────────

replen_sql = f"""
SELECT
    r.store_id,
    s.format_cd,
    COUNT(*)                                                                AS total_orders,
    SUM(CASE WHEN r.replenishment_fulfilled_flag = 0 AND
                  r.replenishment_sla_hrs > {replen_sla_hrs} THEN 1 ELSE 0 END) AS sla_breach_count,
    ROUND(AVG(r.replenishment_sla_hrs), 1)                                  AS avg_sla_hrs
FROM {TBL_TMS_REPLEN} r
JOIN {DIM_STORE} s ON r.store_id = s.store_id
WHERE DATE(r.replenishment_order_date) = '{BUSINESS_DATE_STR}'
GROUP BY r.store_id, s.format_cd
HAVING SUM(CASE WHEN r.replenishment_fulfilled_flag = 0 AND
                     r.replenishment_sla_hrs > {replen_sla_hrs} THEN 1 ELSE 0 END) > 0
ORDER BY sla_breach_count DESC
"""

replen_flagged = spark.sql(replen_sql).collect()

# ─────────────────────────────────────────────
# 5. FORWARD LOOK — PROMO ITEMS AT RISK (NEXT 7 DAYS)
# ─────────────────────────────────────────────

forward7_date = (
    __import__("datetime").datetime.strptime(BUSINESS_DATE_STR, "%Y-%m-%d").date()
    + timedelta(days=7)
).strftime("%Y-%m-%d")

promo_risk_sql = f"""
SELECT
    sc.vendor_id,
    sc.sku_id,
    p.product_desc,
    p.category_cd,
    sc.vendor_fill_rate_pct,
    pc.promo_start_date,
    pc.promo_end_date
FROM {TBL_SUPPLY_CHAIN} sc
JOIN {DIM_PRODUCT}        p  ON sc.sku_id = p.sku_id
JOIN {TBL_PROMO_CALENDAR} pc ON sc.sku_id = pc.sku_id
    AND pc.promo_start_date BETWEEN DATE_ADD('{BUSINESS_DATE_STR}', 1) AND '{forward7_date}'
WHERE sc.business_date = '{BUSINESS_DATE_STR}'
  AND sc.vendor_fill_rate_pct < {fill_rate_threshold}
  AND p.category_cd NOT IN ('{EXCLUDED_CAT_SQL}')
ORDER BY pc.promo_start_date ASC
"""

promo_risk_forward = spark.sql(promo_risk_sql).collect()

# ─────────────────────────────────────────────
# 6. BUILD FLAG REPORT
# ─────────────────────────────────────────────

# Top priority — promo fill rate failures (ALWAYS CRITICAL per spec)
if promo_fill_flagged:
    vendor_cat_summary = {}
    for r in promo_fill_flagged:
        key = (r["vendor_id"], r["category_cd"])
        vendor_cat_summary.setdefault(key, []).append(r["vendor_fill_rate_pct"] or 0)

    flag_lines = []
    for (vid, cat), rates in sorted(vendor_cat_summary.items(), key=lambda x: min(x[1])):
        avg_rate = sum(rates) / len(rates)
        flag_lines.append(
            f"  Vendor {vid} ({cat}): {len(rates)} promo SKUs, avg fill rate {avg_rate:.1%}"
        )

    report.add_flag(
        severity="CRITICAL",
        title="Vendor Fill Rate Below Threshold on Active Promo Items",
        detail=(
            f"{len(promo_fill_flagged)} promo item × vendor combinations below "
            f"{fill_rate_threshold:.0%} fill rate:\n" + "\n".join(flag_lines[:10])
        ),
        metric=f"{len(promo_fill_flagged)} promo OOS supply-side failures",
        impact="Promotional inventory at risk. Markdown spend running on items that may not be on shelf.",
        forward_look=(
            "Cross-reference Agent 4 (promo OOS) and Agent 7 (markdown spend above LY). "
            "This is the supply chain root cause of the promo execution failure chain."
        ),
    )

# Non-promo fill rate failures
if fill_rate_flagged:
    worst = fill_rate_flagged[:5]
    lines = [
        f"  Vendor {r['vendor_id']} ({r['category_cd']}): fill rate {r['vendor_fill_rate_pct']:.1%} "
        f"(7d avg {r['avg_fill_rate_7d'] or 0:.1%})"
        for r in worst
    ]
    report.add_flag(
        severity="WATCH",
        title="Non-Promo Vendor Fill Rate Below Threshold",
        detail=(
            f"{len(fill_rate_flagged)} vendor/SKU combinations with fill rate <{fill_rate_threshold:.0%}:\n"
            + "\n".join(lines)
        ),
        metric=f"{len(fill_rate_flagged)} vendor/SKU fill rate failures",
        forward_look="Monitor for OOS risk building in Agent 4 over next 3-5 days.",
    )

# PO delays
if po_delays:
    promo_po_delays = [r for r in po_delays if r["promo_flag"]]
    nonp_po_delays  = [r for r in po_delays if not r["promo_flag"]]

    if promo_po_delays:
        worst_promo = promo_po_delays[:5]
        lines = [
            f"  PO {r['po_id']} ({r['category_cd']}): {r['delay_days']} days delayed"
            for r in worst_promo
        ]
        report.add_flag(
            severity="CRITICAL",
            title=f"Inbound PO Delays — {len(promo_po_delays)} Promo POs Past Expected Receipt",
            detail="\n".join(lines),
            metric=f"{len(promo_po_delays)} promo POs delayed",
            forward_look="Delayed promo POs feed directly into OOS risk for Agent 4.",
        )

    if nonp_po_delays:
        report.add_flag(
            severity="WATCH",
            title=f"{len(nonp_po_delays)} Non-Promo POs Past Expected Receipt Date",
            detail=f"Average delay: {sum(r['delay_days'] for r in nonp_po_delays) / len(nonp_po_delays):.1f} days.",
            metric=f"{len(nonp_po_delays)} POs delayed",
        )

# Replenishment SLA
if replen_flagged:
    total_breaches = sum(r["sla_breach_count"] for r in replen_flagged)
    report.add_flag(
        severity="WATCH",
        title=f"Store Replenishment Orders Breaching {replen_sla_hrs}hr SLA",
        detail=(
            f"{len(replen_flagged)} stores with replenishment SLA breaches. "
            f"Total unfulfilled orders past SLA: {total_breaches}."
        ),
        metric=f"{total_breaches} orders past {replen_sla_hrs}hr SLA across {len(replen_flagged)} stores",
        forward_look="SLA breaches in replenishment build into OSA failures 24-48 hours later.",
    )

# Forward look — upcoming promo risk
if promo_risk_forward:
    promo_risk_lines = []
    for r in promo_risk_forward[:8]:
        promo_risk_lines.append(
            f"  SKU {r['sku_id']} ({r['category_cd']}): promo starts {r['promo_start_date']}, "
            f"current fill rate {r['vendor_fill_rate_pct']:.1%}"
        )
    report.add_flag(
        severity="WATCH",
        title=f"Promo Fill Rate Risk — Next 7 Days",
        detail=(
            f"{len(promo_risk_forward)} upcoming promo items with current fill rate <{fill_rate_threshold:.0%}:\n"
            + "\n".join(promo_risk_lines)
        ),
        metric=f"{len(promo_risk_forward)} at-risk promo items",
        forward_look="Escalate to vendor procurement before promo launch to avoid execution failure.",
    )

# ─────────────────────────────────────────────
# 7. OUTPUT
# ─────────────────────────────────────────────

agent_8_output = report.render()
print(agent_8_output)

write_flag_history(spark, TBL_FLAG_HISTORY, 8, report, BUSINESS_DATE_STR)

dbutils.jobs.taskValues.set(key="output",                 value=agent_8_output)
dbutils.jobs.taskValues.set(key="status_color",           value=report.status_color)
dbutils.jobs.taskValues.set(key="critical_count",         value=report.critical_count)
dbutils.jobs.taskValues.set(key="watch_count",            value=report.watch_count)
dbutils.jobs.taskValues.set(key="win_count",              value=report.win_count)
dbutils.jobs.taskValues.set(key="promo_fill_failures",    value=len(promo_fill_flagged))
dbutils.jobs.taskValues.set(key="promo_risk_forward",     value=len(promo_risk_forward))
