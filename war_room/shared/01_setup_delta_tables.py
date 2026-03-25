# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## War Room — Delta Table Setup
# MAGIC
# MAGIC Run **once** to create the War Room catalog, database, and Delta tables.
# MAGIC
# MAGIC Required permissions: CREATE CATALOG, CREATE DATABASE, CREATE TABLE in Unity Catalog.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# ─────────────────────────────────────────────
# CREATE CATALOG AND DATABASE (idempotent)
# ─────────────────────────────────────────────

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS war_room COMMENT 'War Room triage system output tables'")
spark.sql(f"USE DATABASE war_room")

print(f"Catalog '{CATALOG}' and database 'war_room' are ready.")

# ─────────────────────────────────────────────
# FLAG HISTORY TABLE
# Stores every agent flag from every run for historical trending
# ─────────────────────────────────────────────

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_FLAG_HISTORY} (
    business_date  DATE         NOT NULL  COMMENT 'Business date of the analysis run',
    agent_id       INT          NOT NULL  COMMENT 'Agent number 1-8',
    agent_name     STRING       NOT NULL  COMMENT 'Agent domain name',
    severity       STRING       NOT NULL  COMMENT 'CRITICAL | WATCH | WIN | GREEN',
    flag_title     STRING       NOT NULL  COMMENT 'Short flag title',
    flag_detail    STRING                 COMMENT 'Full flag detail text',
    impact         STRING                 COMMENT 'Quantified dollar or pct impact if available',
    created_ts     DATE                   COMMENT 'Date this row was written'
)
USING DELTA
PARTITIONED BY (business_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.enableChangeDataFeed'       = 'true'
)
COMMENT 'Daily War Room flag history — all agent flags across all runs'
""")

print(f"Table {TBL_FLAG_HISTORY} is ready.")

# ─────────────────────────────────────────────
# BRIEFING HISTORY TABLE
# Stores the full executive briefing text for each run
# ─────────────────────────────────────────────

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_BRIEFING_HISTORY} (
    business_date       DATE    NOT NULL  COMMENT 'Business date of the briefing',
    briefing_text       STRING            COMMENT 'Full executive briefing text from Commander',
    total_flags         INT               COMMENT 'Total flag count for this run',
    critical_count      INT               COMMENT 'Critical flag count',
    watch_count         INT               COMMENT 'Watch flag count',
    win_count           INT               COMMENT 'Win flag count',
    agent_colors        STRING            COMMENT 'JSON map of agent_id to RED|YELLOW|GREEN',
    created_ts          DATE              COMMENT 'Date this row was written'
)
USING DELTA
PARTITIONED BY (business_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
)
COMMENT 'Daily War Room Commander executive briefings'
""")

print(f"Table {TBL_BRIEFING_HISTORY} is ready.")

# ─────────────────────────────────────────────
# VERIFY
# ─────────────────────────────────────────────

display(spark.sql(f"SHOW TABLES IN {CATALOG}.war_room"))
print("War Room Delta tables setup complete.")
