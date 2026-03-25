# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Agent 4 — Merchandising & Inventory
# MAGIC **Domain:** On-Shelf Availability, OOS, Promo Item Tracking
# MAGIC
# MAGIC - Grain: Store × SKU × Day
# MAGIC - Exception priority: Promo OOS → Top-100 velocity OOS → Category OSA% below threshold
# MAGIC - OSA threshold: 95%
# MAGIC - Comparison: YoY + 7-day rolling

# COMMAND ----------

# MAGIC %run ../shared/00_config

# COMMAND ----------

# MAGIC %run ../shared/00_utils

# COMMAND ----------

from pyspark.sql import functions as F

report = FlagReport(4, "Merchandising & Inventory", BUSINESS_DATE_STR)

osa_threshold = THRESHOLDS["osa_threshold_pct"]

# ─────────────────────────────────────────────
# 1. PROMO ITEMS — HIGHEST PRIORITY
# ─────────────────────────────────────────────

promo_oos_sql = f"""
SELECT
    o.store_id,
    o.sku_id,
    p.product_desc,
    p.category_cd,
    p.department_cd,
    p.velocity_rank,
    o.osa_pct,
    o.on_hand_units,
    o.shelf_capacity,
    o.business_date,
    pc.promo_type,
    pc.promo_start_date,
    pc.promo_end_date,
    -- Days of supply: on_hand / 28-day avg daily velocity
    ROUND(o.on_hand_units / NULLIF(vel.avg_daily_sales, 0), 1) AS days_of_supply
FROM {TBL_OSA_GOLD} o
JOIN {DIM_PRODUCT} p     ON o.sku_id = p.sku_id
JOIN {TBL_PROMO_CALENDAR} pc ON o.sku_id = pc.sku_id
    AND '{BUSINESS_DATE_STR}' BETWEEN pc.promo_start_date AND pc.promo_end_date
LEFT JOIN (
    SELECT sku_id, AVG(units_sold) AS avg_daily_sales
    FROM {DB_GOLD}.daily_sales_velocity
    WHERE business_date BETWEEN DATE_ADD('{BUSINESS_DATE_STR}', -28) AND DATE_ADD('{BUSINESS_DATE_STR}', -1)
    GROUP BY sku_id
) vel ON o.sku_id = vel.sku_id
WHERE o.business_date = '{BUSINESS_DATE_STR}'
  AND o.osa_pct < {osa_threshold}
  AND p.category_cd NOT IN ('{EXCLUDED_CAT_SQL}')
ORDER BY o.osa_pct ASC
"""

promo_oos_df = spark.sql(promo_oos_sql).cache()
promo_oos_rows = promo_oos_df.collect()

# ─────────────────────────────────────────────
# 2. TOP-100 VELOCITY SKUs OOS  (non-promo)
# ─────────────────────────────────────────────

top_velocity_oos_sql = f"""
SELECT
    o.store_id,
    o.sku_id,
    p.product_desc,
    p.category_cd,
    p.department_cd,
    p.velocity_rank,
    o.osa_pct,
    o.on_hand_units,
    ROUND(o.on_hand_units / NULLIF(vel.avg_daily_sales, 0), 1) AS days_of_supply
FROM {TBL_OSA_GOLD} o
JOIN {DIM_PRODUCT} p ON o.sku_id = p.sku_id
LEFT JOIN (
    SELECT sku_id, AVG(units_sold) AS avg_daily_sales
    FROM {DB_GOLD}.daily_sales_velocity
    WHERE business_date BETWEEN DATE_ADD('{BUSINESS_DATE_STR}', -28) AND DATE_ADD('{BUSINESS_DATE_STR}', -1)
    GROUP BY sku_id
) vel ON o.sku_id = vel.sku_id
LEFT JOIN {TBL_PROMO_CALENDAR} pc ON o.sku_id = pc.sku_id
    AND '{BUSINESS_DATE_STR}' BETWEEN pc.promo_start_date AND pc.promo_end_date
WHERE o.business_date = '{BUSINESS_DATE_STR}'
  AND o.osa_pct < {osa_threshold}
  AND p.velocity_rank <= 100
  AND p.category_cd NOT IN ('{EXCLUDED_CAT_SQL}')
  AND pc.sku_id IS NULL    -- exclude promo items (already covered above)
ORDER BY p.velocity_rank ASC
"""

top_velocity_oos = spark.sql(top_velocity_oos_sql).collect()

# ─────────────────────────────────────────────
# 3. CATEGORY OSA % — AGGREGATE VIEW
# ─────────────────────────────────────────────

category_osa_sql = f"""
SELECT
    p.category_cd,
    p.department_cd,
    s.format_cd,
    ROUND(AVG(o.osa_pct), 4)   AS avg_osa_pct,
    COUNT(DISTINCT o.sku_id)   AS sku_count,
    COUNT(DISTINCT o.store_id) AS store_count
FROM {TBL_OSA_GOLD} o
JOIN {DIM_PRODUCT} p ON o.sku_id = p.sku_id
JOIN {DIM_STORE}   s ON o.store_id = s.store_id
WHERE o.business_date = '{BUSINESS_DATE_STR}'
  AND p.category_cd NOT IN ('{EXCLUDED_CAT_SQL}')
GROUP BY p.category_cd, p.department_cd, s.format_cd
HAVING AVG(o.osa_pct) < {osa_threshold}
ORDER BY avg_osa_pct ASC
"""

category_osa_flagged = spark.sql(category_osa_sql).collect()

# ─────────────────────────────────────────────
# 4. YoY COMPARISON — category OSA vs same day LY
# ─────────────────────────────────────────────

category_osa_ly_sql = f"""
SELECT
    p.category_cd,
    s.format_cd,
    ROUND(AVG(o.osa_pct), 4) AS avg_osa_pct_ly
FROM {TBL_OSA_GOLD} o
JOIN {DIM_PRODUCT} p ON o.sku_id = p.sku_id
JOIN {DIM_STORE}   s ON o.store_id = s.store_id
WHERE o.business_date = '{BUSINESS_DATE_LY}'
  AND p.category_cd NOT IN ('{EXCLUDED_CAT_SQL}')
GROUP BY p.category_cd, s.format_cd
"""

osa_ly_map = {
    (r["category_cd"], r["format_cd"]): r["avg_osa_pct_ly"]
    for r in spark.sql(category_osa_ly_sql).collect()
}

# 7-day rolling OSA per category
osa_7d_sql = f"""
SELECT
    p.category_cd,
    s.format_cd,
    ROUND(AVG(o.osa_pct), 4) AS avg_osa_7d
FROM {TBL_OSA_GOLD} o
JOIN {DIM_PRODUCT} p ON o.sku_id = p.sku_id
JOIN {DIM_STORE}   s ON o.store_id = s.store_id
WHERE o.business_date BETWEEN DATE_ADD('{BUSINESS_DATE_STR}', -7) AND DATE_ADD('{BUSINESS_DATE_STR}', -1)
  AND p.category_cd NOT IN ('{EXCLUDED_CAT_SQL}')
GROUP BY p.category_cd, s.format_cd
"""

osa_7d_map = {
    (r["category_cd"], r["format_cd"]): r["avg_osa_7d"]
    for r in spark.sql(osa_7d_sql).collect()
}

# ─────────────────────────────────────────────
# 5. BUILD FLAG REPORT
# ─────────────────────────────────────────────

# Priority 1 — Promo OOS
if promo_oos_rows:
    # Summarise by category
    promo_cats = {}
    for r in promo_oos_rows:
        cat = r["category_cd"]
        promo_cats.setdefault(cat, []).append(r)

    top_promo_lines = []
    for cat, rows in sorted(promo_cats.items(), key=lambda x: len(x[1]), reverse=True)[:5]:
        avg_osa  = sum(r["osa_pct"] or 0 for r in rows) / len(rows)
        avg_dos  = sum(r["days_of_supply"] or 0 for r in rows) / len(rows)
        top_promo_lines.append(
            f"  {cat}: {len(rows)} store/SKU combos — avg OSA {avg_osa:.1%}, avg days of supply {avg_dos:.1f}"
        )

    report.add_flag(
        severity="CRITICAL",
        title="Active Promo Items OOS",
        detail=(
            f"{len(promo_oos_rows)} promo item × store combinations below {osa_threshold:.0%} OSA.\n"
            + "\n".join(top_promo_lines)
        ),
        metric=f"{len(promo_oos_rows)} promo OOS combos across {len(promo_cats)} categories",
        impact="Promotional spend executing against unavailable inventory.",
        forward_look=(
            "Cross-reference Agent 8 for vendor fill rate on these promo items. "
            "Cross-reference Agent 7 for markdown spend running above LY."
        ),
    )

# Priority 2 — Top-100 velocity OOS
if top_velocity_oos:
    top_sku_lines = [
        f"  SKU {r['sku_id']} (rank {r['velocity_rank']}, {r['category_cd']}): "
        f"OSA {r['osa_pct']:.1%}, {r['days_of_supply'] or 0:.1f} days supply"
        for r in top_velocity_oos[:10]
    ]
    report.add_flag(
        severity="CRITICAL",
        title="Top-100 Velocity SKUs Below OSA Threshold",
        detail=(
            f"{len(top_velocity_oos)} high-velocity SKUs below {osa_threshold:.0%} OSA:\n"
            + "\n".join(top_sku_lines)
        ),
        metric=f"{len(top_velocity_oos)} velocity SKUs flagged",
        forward_look="Velocity SKU OOS directly impacts comp sales — feed to Agent 1 context.",
    )

# Priority 3 — Category OSA
if category_osa_flagged:
    cat_lines = []
    for r in category_osa_flagged[:8]:
        key    = (r["category_cd"], r["format_cd"])
        osa_ly = osa_ly_map.get(key)
        osa_7d = osa_7d_map.get(key)
        osa_str = f"OSA: {r['avg_osa_pct']:.1%}"
        if osa_ly:
            osa_str += f" (LY: {osa_ly:.1%}, Δ {r['avg_osa_pct'] - osa_ly:+.1%})"
        if osa_7d:
            osa_str += f" | 7d avg: {osa_7d:.1%}"
        cat_lines.append(f"  {r['category_cd']} ({r['format_cd']}): {osa_str}")

    report.add_flag(
        severity="WATCH",
        title="Category-Level OSA Below 95% Threshold",
        detail=(
            f"{len(category_osa_flagged)} categories below OSA threshold:\n"
            + "\n".join(cat_lines)
        ),
        metric=f"{len(category_osa_flagged)} categories flagged",
        forward_look=(
            "Cross-reference Agent 2 for customer satisfaction scores in flagged categories."
        ),
    )

# ─────────────────────────────────────────────
# 6. OUTPUT
# ─────────────────────────────────────────────

# Expose flagged categories for cross-agent use by Commander
flagged_osa_cats = list({r["category_cd"] for r in (promo_oos_rows + top_velocity_oos + category_osa_flagged)})

agent_4_output = report.render()
print(agent_4_output)

write_flag_history(spark, TBL_FLAG_HISTORY, 4, report, BUSINESS_DATE_STR)

dbutils.jobs.taskValues.set(key="output",            value=agent_4_output)
dbutils.jobs.taskValues.set(key="status_color",      value=report.status_color)
dbutils.jobs.taskValues.set(key="critical_count",    value=report.critical_count)
dbutils.jobs.taskValues.set(key="watch_count",       value=report.watch_count)
dbutils.jobs.taskValues.set(key="win_count",         value=report.win_count)
dbutils.jobs.taskValues.set(key="flagged_osa_cats",  value=flagged_osa_cats)
dbutils.jobs.taskValues.set(key="promo_oos_count",   value=len(promo_oos_rows))
