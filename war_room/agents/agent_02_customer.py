# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Agent 2 — Customer
# MAGIC **Domain:** NPS, Satisfaction, Loyalty Redemption, Household Visit Frequency
# MAGIC
# MAGIC - Grain: Household × Day (behavioral); Transaction × Survey (NPS/Sat)
# MAGIC - Min 30 responses before flagging any category score
# MAGIC - Comparison: YoY primary; 28-day rolling secondary (trend divergence)
# MAGIC - NPS window: 7-day rolling vs same 7-day LY by category and channel

# COMMAND ----------

# MAGIC %run ../shared/00_config

# COMMAND ----------

# MAGIC %run ../shared/00_utils

# COMMAND ----------

from pyspark.sql import functions as F
from collections import Counter
import re

report = FlagReport(2, "Customer", BUSINESS_DATE_STR)

# ─────────────────────────────────────────────
# 1. HOUSEHOLD BEHAVIORAL — YoY
# ─────────────────────────────────────────────

behavioral_sql = f"""
SELECT
    th.segment_cd,
    COUNT(DISTINCT th.household_id)                              AS hh_count,
    SUM(th.visit_count_ty)                                       AS visits_ty,
    SUM(th.visit_count_ly)                                       AS visits_ly,
    SUM(th.spend_ty)                                             AS spend_ty,
    SUM(th.spend_ly)                                             AS spend_ly,
    ROUND(AVG(th.basket_size_ty), 2)                             AS basket_ty,
    ROUND(AVG(th.basket_size_ly), 2)                             AS basket_ly,
    SUM(CASE WHEN th.redemption_flag = 1 THEN th.redemption_value ELSE 0 END) AS redemption_value,
    ROUND((SUM(th.spend_ty) - SUM(th.spend_ly)) / NULLIF(SUM(th.spend_ly), 0), 4) AS spend_yoy_pct,
    ROUND((SUM(th.visit_count_ty) - SUM(th.visit_count_ly)) / NULLIF(SUM(th.visit_count_ly), 0), 4) AS visit_yoy_pct
FROM {TBL_TXN_HOUSEHOLD} th
JOIN {DB_DIM}.household_segment hs ON th.household_id = hs.household_id
WHERE th.business_date = '{BUSINESS_DATE_STR}'
GROUP BY th.segment_cd
ORDER BY spend_ty DESC
"""

behavioral_df = spark.sql(behavioral_sql).collect()

# ─────────────────────────────────────────────
# 2. TOTAL COMPANY COMP — for segment decomposition
# ─────────────────────────────────────────────

total_comp_sql = f"""
SELECT
    SUM(spend_ty)    AS total_spend_ty,
    SUM(spend_ly)    AS total_spend_ly,
    (SUM(spend_ty) - SUM(spend_ly)) AS total_delta
FROM {TBL_TXN_HOUSEHOLD}
WHERE business_date = '{BUSINESS_DATE_STR}'
"""

total_row = spark.sql(total_comp_sql).collect()[0]
total_delta = total_row["total_delta"] or 0

# ─────────────────────────────────────────────
# 3. 28-DAY ROLLING — TREND DIVERGENCE
# ─────────────────────────────────────────────

rolling28_sql = f"""
SELECT
    segment_cd,
    ROUND(
        (SUM(spend_ty) - SUM(spend_ly)) / NULLIF(SUM(spend_ly), 0), 4
    ) AS spend_yoy_28d
FROM {TBL_TXN_HOUSEHOLD}
WHERE business_date BETWEEN DATE_ADD('{BUSINESS_DATE_STR}', -27) AND '{BUSINESS_DATE_STR}'
GROUP BY segment_cd
"""

rolling28 = {r["segment_cd"]: r["spend_yoy_28d"] for r in spark.sql(rolling28_sql).collect()}

# ─────────────────────────────────────────────
# 4. NPS / SATISFACTION — 7-DAY ROLLING BY CATEGORY & CHANNEL
# ─────────────────────────────────────────────

nps_window_sql = f"""
SELECT
    n.survey_category,
    n.channel,
    COUNT(*)                            AS response_count,
    ROUND(AVG(n.nps_score), 2)          AS avg_nps_ty,
    ROUND(AVG(n.satisfaction_score), 2) AS avg_sat_ty
FROM {TBL_NPS_SAT} n
WHERE n.response_timestamp BETWEEN DATE_ADD('{BUSINESS_DATE_STR}', -6) AND '{BUSINESS_DATE_STR}'
GROUP BY n.survey_category, n.channel
"""

nps_window_ly_sql = f"""
SELECT
    n.survey_category,
    n.channel,
    ROUND(AVG(n.nps_score), 2)          AS avg_nps_ly,
    ROUND(AVG(n.satisfaction_score), 2) AS avg_sat_ly
FROM {TBL_NPS_SAT} n
WHERE n.response_timestamp BETWEEN DATE_ADD('{BUSINESS_DATE_LY}', -6) AND '{BUSINESS_DATE_LY}'
GROUP BY n.survey_category, n.channel
"""

nps_ty = spark.sql(nps_window_sql)
nps_ly = spark.sql(nps_window_ly_sql)

nps_joined = (
    nps_ty
    .join(nps_ly, on=["survey_category", "channel"], how="left")
    .withColumn("nps_delta",  F.col("avg_nps_ty")  - F.col("avg_nps_ly"))
    .withColumn("sat_delta",  F.col("avg_sat_ty")  - F.col("avg_sat_ly"))
).collect()

min_sample = THRESHOLDS["nps_min_sample"]

# ─────────────────────────────────────────────
# 5. FREE TEXT — SURFACE THEMES FOR FLAGGED CATEGORIES
# ─────────────────────────────────────────────

def get_free_text_themes(spark, category: str) -> str:
    """Return top recurring themes from free_text_comment for a flagged category."""
    try:
        ft_sql = f"""
            SELECT free_text_comment
            FROM {TBL_NPS_SAT}
            WHERE response_timestamp BETWEEN DATE_ADD('{BUSINESS_DATE_STR}', -6) AND '{BUSINESS_DATE_STR}'
              AND survey_category = '{category}'
              AND free_text_comment IS NOT NULL
              AND LENGTH(TRIM(free_text_comment)) > 10
            LIMIT 200
        """
        comments = [r["free_text_comment"].lower() for r in spark.sql(ft_sql).collect()]
        # Simple keyword frequency theme extraction
        keywords = []
        for c in comments:
            words = re.findall(r"\b[a-z]{4,}\b", c)
            keywords.extend(w for w in words if w not in {
                "that", "this", "with", "have", "been", "very", "they", "were",
                "your", "from", "just", "also", "when", "then", "there", "about"
            })
        top = Counter(keywords).most_common(8)
        return ", ".join(f"{w} ({n})" for w, n in top)
    except Exception:
        return "free text unavailable"

# ─────────────────────────────────────────────
# 6. BUILD FLAG REPORT
# ─────────────────────────────────────────────

threshold_segment = THRESHOLDS["customer_segment_comp_pct"]

# Behavioral — segment-level flags
for row in behavioral_df:
    seg           = row["segment_cd"]
    spend_yoy     = row["spend_yoy_pct"] or 0.0
    visit_yoy     = row["visit_yoy_pct"] or 0.0
    spend_delta   = (row["spend_ty"] or 0) - (row["spend_ly"] or 0)
    rolling28_pct = rolling28.get(seg, 0.0) or 0.0

    # Does this segment explain >50% of total comp variance?
    if total_delta != 0 and abs(spend_delta / total_delta) > threshold_segment:
        severity = "CRITICAL" if abs(spend_yoy) > 0.05 else "WATCH"
        report.add_flag(
            severity=severity,
            title=f"{seg} Segment Driving Comp Variance",
            detail=(
                f"{seg} segment accounts for {abs(spend_delta / total_delta):.0%} of total "
                f"comp dollar variance. Spend: {spend_yoy:+.1%} YoY; "
                f"Visits: {visit_yoy:+.1%} YoY. "
                f"28-day rolling trend: {rolling28_pct:+.1%}."
            ),
            metric=f"Spend YoY: {spend_yoy:+.1%}  |  Visit YoY: {visit_yoy:+.1%}",
            impact=f"${spend_delta:+,.0f} vs LY",
            forward_look=(
                "Determine whether decline is traffic (visits) or basket-driven. "
                "Cross-reference Agent 1 comp decomposition."
            ),
        )

# NPS/Sat — category and channel flags
flagged_categories = set()
for row in nps_joined:
    cat     = row["survey_category"]
    channel = row["channel"]
    count   = row["response_count"] or 0
    nps_d   = row["nps_delta"]
    sat_d   = row["sat_delta"]
    avg_nps = row["avg_nps_ty"]
    avg_sat = row["avg_sat_ty"]

    if count < min_sample:
        report.add_data_quality_note(
            f"NPS/Sat suppressed for {cat} / {channel}: only {count} responses (min {min_sample} required)."
        )
        continue

    if (nps_d is not None and nps_d < -3) or (sat_d is not None and sat_d < -0.2):
        flagged_categories.add(cat)
        themes = get_free_text_themes(spark, cat)
        report.add_flag(
            severity="CRITICAL" if (nps_d or 0) < -5 else "WATCH",
            title=f"{cat} ({channel}) Satisfaction Declining vs LY",
            detail=(
                f"7-day rolling NPS: {avg_nps:.1f} (Δ {nps_d:+.1f} vs LY). "
                f"Satisfaction: {avg_sat:.2f} (Δ {sat_d:+.2f} vs LY). "
                f"Sample: {count} responses. "
                f"Free-text themes: {themes}."
            ),
            metric=f"NPS Δ: {nps_d:+.1f}  |  Sat Δ: {sat_d:+.2f}",
            forward_look=(
                f"Cross-reference Agent 4 for OOS in {cat} category — "
                "strongest cross-agent signal if aligned."
            ),
        )

# ─────────────────────────────────────────────
# 7. OUTPUT
# ─────────────────────────────────────────────

agent_2_output = report.render()
print(agent_2_output)

write_flag_history(spark, TBL_FLAG_HISTORY, 2, report, BUSINESS_DATE_STR)

dbutils.jobs.taskValues.set(key="output",            value=agent_2_output)
dbutils.jobs.taskValues.set(key="status_color",      value=report.status_color)
dbutils.jobs.taskValues.set(key="critical_count",    value=report.critical_count)
dbutils.jobs.taskValues.set(key="watch_count",       value=report.watch_count)
dbutils.jobs.taskValues.set(key="win_count",         value=report.win_count)
dbutils.jobs.taskValues.set(key="flagged_nps_cats",  value=list(flagged_categories))
