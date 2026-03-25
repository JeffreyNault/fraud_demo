# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Agent 7 — Finance
# MAGIC **Domain:** Gross Margin Rate & Dollars, Promotional Markdown Spend
# MAGIC
# MAGIC - Grain: Category × Department × Day
# MAGIC - Exception: Margin rate deviates >50bps vs LY or plan
# MAGIC - Always express in both rate AND dollar impact
# MAGIC - Markdown spend flag: >10% above LY
# MAGIC - Fuel and Pharmacy flagged independently if data available

# COMMAND ----------

# MAGIC %run ../shared/00_config

# COMMAND ----------

# MAGIC %run ../shared/00_utils

# COMMAND ----------

from pyspark.sql import functions as F

report = FlagReport(7, "Finance", BUSINESS_DATE_STR)

margin_bps_threshold = THRESHOLDS["margin_rate_flag_bps"]
markdown_threshold   = THRESHOLDS["markdown_flag_pct"]

# ─────────────────────────────────────────────
# 1. GROSS MARGIN BY CATEGORY — YoY AND PLAN
# ─────────────────────────────────────────────

finance_sql = f"""
SELECT
    f.category_cd,
    f.department_cd,
    f.gross_margin_rate_ty,
    f.gross_margin_rate_ly,
    f.gross_margin_rate_plan,
    f.gross_margin_dollars_ty,
    f.gross_margin_dollars_ly,
    f.gross_margin_dollars_plan,
    f.promo_markdown_spend_ty,
    f.promo_markdown_spend_ly,
    -- Rate deviation in bps
    ROUND((f.gross_margin_rate_ty - f.gross_margin_rate_ly)   * 10000, 1) AS gm_rate_bps_vs_ly,
    ROUND((f.gross_margin_rate_ty - f.gross_margin_rate_plan) * 10000, 1) AS gm_rate_bps_vs_plan,
    -- Dollar impact
    (f.gross_margin_dollars_ty - f.gross_margin_dollars_ly)               AS gm_dollar_delta_ly,
    (f.gross_margin_dollars_ty - f.gross_margin_dollars_plan)             AS gm_dollar_delta_plan,
    -- Markdown variance
    ROUND((f.promo_markdown_spend_ty - f.promo_markdown_spend_ly) / NULLIF(f.promo_markdown_spend_ly, 0), 4) AS markdown_yoy_pct
FROM {TBL_FINANCE_GOLD} f
WHERE f.business_date = '{BUSINESS_DATE_STR}'
  AND f.category_cd NOT IN ('{EXCLUDED_CAT_SQL}')
  AND f.category_cd NOT IN ('FUEL', 'PHARMACY')   -- handled separately below
"""

finance_rows = spark.sql(finance_sql).collect()

# Fuel and Pharmacy (independent blocks)
fuel_pharma_sql = f"""
SELECT
    f.category_cd,
    f.gross_margin_rate_ty,
    f.gross_margin_rate_ly,
    f.gross_margin_dollars_ty,
    f.gross_margin_dollars_ly,
    ROUND((f.gross_margin_rate_ty - f.gross_margin_rate_ly) * 10000, 1) AS gm_rate_bps_vs_ly,
    (f.gross_margin_dollars_ty - f.gross_margin_dollars_ly)             AS gm_dollar_delta_ly
FROM {TBL_FINANCE_GOLD} f
WHERE f.business_date = '{BUSINESS_DATE_STR}'
  AND f.category_cd IN ('FUEL', 'PHARMACY')
"""

fuel_pharma_rows = spark.sql(fuel_pharma_sql).collect()

# ─────────────────────────────────────────────
# 2. COMPANY-LEVEL TOTALS FOR CONTEXT
# ─────────────────────────────────────────────

totals_sql = f"""
SELECT
    SUM(gross_margin_dollars_ty)    AS total_gm_ty,
    SUM(gross_margin_dollars_ly)    AS total_gm_ly,
    SUM(promo_markdown_spend_ty)    AS total_markdown_ty,
    SUM(promo_markdown_spend_ly)    AS total_markdown_ly,
    ROUND(
        SUM(gross_margin_dollars_ty) / NULLIF(SUM(net_sales_ty), 0), 4
    )                               AS company_gm_rate_ty,
    ROUND(
        SUM(gross_margin_dollars_ly) / NULLIF(SUM(net_sales_ly), 0), 4
    )                               AS company_gm_rate_ly
FROM {TBL_FINANCE_GOLD}
WHERE business_date = '{BUSINESS_DATE_STR}'
  AND category_cd NOT IN ('{EXCLUDED_CAT_SQL}')
  AND category_cd NOT IN ('FUEL', 'PHARMACY')
"""

totals = spark.sql(totals_sql).collect()[0]
total_gm_delta      = (totals["total_gm_ty"] or 0) - (totals["total_gm_ly"] or 0)
company_gm_rate_ty  = totals["company_gm_rate_ty"] or 0
company_gm_rate_ly  = totals["company_gm_rate_ly"] or 0
company_bps_vs_ly   = (company_gm_rate_ty - company_gm_rate_ly) * 10000
total_markdown_ty   = totals["total_markdown_ty"] or 0
total_markdown_ly   = totals["total_markdown_ly"] or 0
total_markdown_delta = total_markdown_ty - total_markdown_ly
markdown_yoy_total   = total_markdown_delta / (total_markdown_ly or 1)

# ─────────────────────────────────────────────
# 3. BUILD FLAG REPORT
# ─────────────────────────────────────────────

# Company-level margin summary flag (if material)
if abs(company_bps_vs_ly) > margin_bps_threshold:
    direction = "below" if company_bps_vs_ly < 0 else "above"
    report.add_flag(
        severity="CRITICAL" if abs(company_bps_vs_ly) > margin_bps_threshold * 2 else "WATCH",
        title=f"Company Gross Margin Rate {direction.upper()} LY",
        detail=(
            f"Company gross margin rate: {company_gm_rate_ty:.1%} vs {company_gm_rate_ly:.1%} LY "
            f"({company_bps_vs_ly:+.0f}bps). Total GM dollar variance: ${total_gm_delta:+,.0f}."
        ),
        metric=f"Rate: {company_bps_vs_ly:+.0f}bps vs LY  |  Dollars: ${total_gm_delta:+,.0f}",
        impact=f"${total_gm_delta:+,.0f} vs LY",
        forward_look=(
            "Cross-reference Agent 4 for OOS/promo execution failures and "
            "Agent 8 for supply chain cost drivers."
        ),
    )

# Category-level flags
margin_flagged_cats = []
for row in finance_rows:
    cat        = row["category_cd"]
    dept       = row["department_cd"]
    bps_vs_ly  = row["gm_rate_bps_vs_ly"]  or 0
    bps_vs_plan= row["gm_rate_bps_vs_plan"] or 0
    dollar_delta = row["gm_dollar_delta_ly"] or 0
    rate_ty    = row["gross_margin_rate_ty"] or 0
    rate_ly    = row["gross_margin_rate_ly"] or 0
    markdown_yoy = row["markdown_yoy_pct"]  or 0

    margin_flag     = abs(bps_vs_ly) > margin_bps_threshold or abs(bps_vs_plan) > margin_bps_threshold
    markdown_flag   = markdown_yoy > markdown_threshold

    if margin_flag:
        margin_flagged_cats.append(cat)
        direction = "compressed" if bps_vs_ly < 0 else "expanded"
        severity = "CRITICAL" if abs(bps_vs_ly) > margin_bps_threshold * 3 else "WATCH"
        detail = (
            f"{cat} ({dept}): margin rate {rate_ty:.1%} vs {rate_ly:.1%} LY "
            f"({bps_vs_ly:+.0f}bps, {direction}). Dollar impact: ${dollar_delta:+,.0f}."
        )
        if abs(bps_vs_plan) > margin_bps_threshold:
            detail += f" vs plan: {bps_vs_plan:+.0f}bps."
        if markdown_flag:
            detail += (
                f" Markdown spend {markdown_yoy:+.1%} vs LY "
                f"(${row['promo_markdown_spend_ty']:,.0f} vs ${row['promo_markdown_spend_ly']:,.0f})."
            )

        report.add_flag(
            severity=severity,
            title=f"{cat} Margin Rate {direction.title()} vs LY",
            detail=detail,
            metric=f"{bps_vs_ly:+.0f}bps vs LY  |  ${dollar_delta:+,.0f}",
            impact=f"${dollar_delta:+,.0f} vs LY",
            forward_look="Cross-reference Agent 4 (promo OOS) and Agent 8 (fill rate) for execution root cause.",
        )
    elif markdown_flag:
        report.add_flag(
            severity="WATCH",
            title=f"{cat} Markdown Spend >10% Above LY",
            detail=(
                f"{cat} ({dept}): promo markdown ${row['promo_markdown_spend_ty']:,.0f} "
                f"vs ${row['promo_markdown_spend_ly']:,.0f} LY ({markdown_yoy:+.1%})."
            ),
            metric=f"Markdown YoY: {markdown_yoy:+.1%}",
            forward_look="Elevated markdown with no margin improvement may signal promo execution issue.",
        )

# Company-level markdown flag if individually categories aren't sufficient
if not margin_flagged_cats and markdown_yoy_total > markdown_threshold:
    report.add_flag(
        severity="WATCH",
        title="Total Company Markdown Spend Above LY",
        detail=(
            f"Company-wide promo markdown spend ${total_markdown_ty:,.0f} vs "
            f"${total_markdown_ly:,.0f} LY ({markdown_yoy_total:+.1%}). "
            f"Incremental spend: ${total_markdown_delta:+,.0f}."
        ),
        metric=f"Markdown YoY: {markdown_yoy_total:+.1%}  |  Incremental: ${total_markdown_delta:+,.0f}",
    )

# Fuel and Pharmacy — independent blocks
for row in fuel_pharma_rows:
    cat = row["category_cd"]
    bps = row["gm_rate_bps_vs_ly"] or 0
    dollars = row["gm_dollar_delta_ly"] or 0
    if abs(bps) > margin_bps_threshold:
        direction = "compressed" if bps < 0 else "expanded"
        report.add_flag(
            severity="WATCH",
            title=f"{cat} — Margin {direction.title()} vs LY (Independent Block)",
            detail=(
                f"{cat}: margin {bps:+.0f}bps vs LY. Dollar variance: ${dollars:+,.0f}. "
                f"Reported independently per global rules."
            ),
            metric=f"{bps:+.0f}bps vs LY",
            impact=f"${dollars:+,.0f}",
        )

# ─────────────────────────────────────────────
# 4. OUTPUT
# ─────────────────────────────────────────────

agent_7_output = report.render()
print(agent_7_output)

write_flag_history(spark, TBL_FLAG_HISTORY, 7, report, BUSINESS_DATE_STR)

dbutils.jobs.taskValues.set(key="output",              value=agent_7_output)
dbutils.jobs.taskValues.set(key="status_color",        value=report.status_color)
dbutils.jobs.taskValues.set(key="critical_count",      value=report.critical_count)
dbutils.jobs.taskValues.set(key="watch_count",         value=report.watch_count)
dbutils.jobs.taskValues.set(key="win_count",           value=report.win_count)
dbutils.jobs.taskValues.set(key="margin_flagged_cats", value=margin_flagged_cats)
dbutils.jobs.taskValues.set(key="total_gm_delta",      value=float(total_gm_delta))
dbutils.jobs.taskValues.set(key="company_bps_vs_ly",   value=float(company_bps_vs_ly))
