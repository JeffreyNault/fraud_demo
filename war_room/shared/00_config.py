# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## War Room — Shared Configuration
# MAGIC Central config for all 8 agents and the Commander.
# MAGIC Import this notebook at the top of each agent: `%run ./shared/00_config`

# COMMAND ----------

from datetime import date, timedelta

# ─────────────────────────────────────────────
# BUSINESS DATE
# ─────────────────────────────────────────────
BUSINESS_DATE = date.today() - timedelta(days=1)   # yesterday
BUSINESS_DATE_STR = BUSINESS_DATE.strftime("%Y-%m-%d")
BUSINESS_DATE_LY  = (BUSINESS_DATE - timedelta(days=365)).strftime("%Y-%m-%d")

# ─────────────────────────────────────────────
# UNITY CATALOG — TABLE REFERENCES
# ─────────────────────────────────────────────
CATALOG   = "retail"
DB_GOLD   = f"{CATALOG}.gold"
DB_MART   = f"{CATALOG}.mart"
DB_DIM    = f"{CATALOG}.dimensions"
DB_WAR    = f"{CATALOG}.war_room"

# Dimension tables
DIM_STORE   = f"{DB_DIM}.dim_store"
DIM_PRODUCT = f"{DB_DIM}.dim_product"

# Agent 1 — Store Operations
TBL_COMP_SALES       = f"{DB_GOLD}.comp_sales_daily"
TBL_LOSS_PREVENTION  = f"{DB_GOLD}.loss_prevention_daily"

# Agent 2 — Customer
TBL_TXN_HOUSEHOLD    = f"{DB_GOLD}.transaction_household"
TBL_HOUSEHOLD_SEG    = f"{DB_DIM}.household_segment"
TBL_NPS_SAT          = f"{DB_GOLD}.nps_satisfaction"

# Agent 3 — Competitors  (Teradata via JDBC)
TERADATA_JDBC_URL    = dbutils.secrets.get(scope="war-room", key="teradata-jdbc-url")
TERADATA_USER        = dbutils.secrets.get(scope="war-room", key="teradata-user")
TERADATA_PASSWORD    = dbutils.secrets.get(scope="war-room", key="teradata-password")
TBL_COMPETITOR_PRICE = "retail_analytics.competitor_price_index"   # Teradata table name

# Agent 4 — Merch & Inventory
TBL_OSA_GOLD         = f"{DB_GOLD}.osa_daily"
TBL_ON_HAND          = f"{DB_GOLD}.inventory_on_hand"
TBL_PROMO_CALENDAR   = f"{DB_MART}.promo_calendar"

# Agent 6 — Digital
TBL_DIGITAL_GOLD     = f"{DB_GOLD}.digital_daily"
TBL_FULFILLMENT      = f"{DB_GOLD}.fulfillment_orders"

# Agent 7 — Finance
TBL_FINANCE_GOLD     = f"{DB_GOLD}.finance_daily"

# Agent 8 — Supply Chain
TBL_SUPPLY_CHAIN     = f"{DB_GOLD}.supply_chain_daily"
TBL_WMS_PO           = f"{DB_GOLD}.wms_purchase_orders"
TBL_TMS_REPLEN       = f"{DB_GOLD}.tms_replenishment"

# Commander — Flag History
TBL_FLAG_HISTORY     = f"{DB_WAR}.flag_history"
TBL_BRIEFING_HISTORY = f"{DB_WAR}.briefing_history"

# ─────────────────────────────────────────────
# EXCLUSION FILTERS  (shared across all agents)
# ─────────────────────────────────────────────
EXCLUDED_CATEGORIES = ("TOBACCO", "GIFT_CARD")
EXCLUDED_CAT_SQL    = "', '".join(EXCLUDED_CATEGORIES)   # for SQL IN clauses

# ─────────────────────────────────────────────
# EXCEPTION THRESHOLDS
# ─────────────────────────────────────────────
THRESHOLDS = {
    # Agent 1
    "store_comp_flag_pct":       0.02,    # ±2% vs plan or LY
    # Agent 2
    "nps_min_sample":            30,      # min responses before flagging
    "customer_segment_comp_pct": 0.50,   # segment explains >50% of comp variance
    # Agent 3
    "price_index_flag_pct":      0.05,   # price index >5% above competitor
    "price_index_wow_flag":      0.02,   # category index shift WoW
    "competitor_data_stale_days": 7,
    # Agent 4
    "osa_threshold_pct":         0.95,   # flag when OSA% < 95%
    # Agent 5
    "gas_price_wow_flag_pct":    0.05,   # gas price moves >5% WoW
    # Agent 6
    "digital_revenue_flag_pct":  0.03,   # ±3% vs LY
    "fulfillment_sla_threshold": 0.95,   # flag when SLA% < 95%
    "bopis_swing_pct":           0.10,   # ±10% vs LY
    # Agent 7
    "margin_rate_flag_bps":      50,     # >50bps deviation
    "markdown_flag_pct":         0.10,   # >10% above LY
    # Agent 8
    "vendor_fill_rate_threshold": 0.95,  # flag below 95%
    "replenishment_sla_hrs":     48,     # flag unfulfilled beyond SLA
}

# ─────────────────────────────────────────────
# CLAUDE API CONFIG
# ─────────────────────────────────────────────
CLAUDE_MODEL          = "claude-sonnet-4-6"
ANTHROPIC_API_KEY     = dbutils.secrets.get(scope="war-room", key="anthropic-api-key")
COMMANDER_MAX_TOKENS  = 4096
AGENT5_MAX_TOKENS     = 2048

# ─────────────────────────────────────────────
# NOTIFICATION CONFIG
# ─────────────────────────────────────────────
TEAMS_WEBHOOK_URL     = dbutils.secrets.get(scope="war-room", key="teams-webhook-url")
EMAIL_DISTRIBUTION    = dbutils.secrets.get(scope="war-room", key="email-distribution")

print(f"War Room Config loaded — Business Date: {BUSINESS_DATE_STR}")
