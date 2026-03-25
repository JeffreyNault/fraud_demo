# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Agent 5 — Weather & External Events
# MAGIC **Domain:** Severe Weather, Precipitation, Gas Prices
# MAGIC
# MAGIC - Data source: Claude web search at runtime (no lakehouse dependency)
# MAGIC - Market list: Pulled from dimStore at runtime
# MAGIC - Silence rule: If no active weather events, output one line and stop
# MAGIC - Gas price: Flag moves >5% WoW vs 30-day trend

# COMMAND ----------

# MAGIC %run ../shared/00_config

# COMMAND ----------

# MAGIC %run ../shared/00_utils

# COMMAND ----------

import json
import anthropic

report = FlagReport(5, "Weather & External Events", BUSINESS_DATE_STR)

# ─────────────────────────────────────────────
# 1. PULL ACTIVE MARKET LIST FROM dimStore
# ─────────────────────────────────────────────

market_sql = f"""
SELECT DISTINCT
    market_id,
    market_name,
    dma_cd,
    state_cd,
    city
FROM {DIM_STORE}
WHERE active_flag = 1
ORDER BY market_name
"""

markets = spark.sql(market_sql).collect()
market_list = [
    f"{r['market_name']}, {r['state_cd']} (DMA {r['dma_cd']})"
    for r in markets
]
market_text = "\n".join(f"- {m}" for m in market_list)

print(f"Querying weather for {len(markets)} markets.")

# ─────────────────────────────────────────────
# 2. CLAUDE WEB SEARCH — WEATHER & GAS PRICES
# ─────────────────────────────────────────────

client = get_anthropic_client(ANTHROPIC_API_KEY)

weather_prompt = f"""You are an analyst for a major regional US retailer. Today is {BUSINESS_DATE_STR}.

Your job is to identify any ACTIVE weather events or gas price changes that would materially impact
customer traffic at retail stores in the following markets:

{market_text}

Please search for:
1. Severe weather events (storms, blizzards, floods, tornadoes, ice storms) active TODAY or in the next 48 hours
   in any of the above markets. Report ONLY markets with material weather events.
2. National average gas price today vs. 7 days ago and vs. 30-day average. Flag if WoW change exceeds 5%.
3. Any major external events (holidays, local events) affecting multiple markets.

SILENCE RULE: If there are no material weather events in any listed market, say exactly:
"No active weather events in monitored markets. Gas price context below if material."

Output format (JSON):
{{
  "weather_events": [
    {{
      "market": "market name",
      "state": "state",
      "event_type": "blizzard|storm|flood|ice|tornado|other",
      "severity": "severe|moderate|minor",
      "impact": "brief description of expected traffic impact",
      "duration": "when it starts/ends",
      "traffic_impact_pct_estimate": optional number like -15 for -15%
    }}
  ],
  "gas_price": {{
    "national_avg_today": 3.45,
    "national_avg_7d_ago": 3.30,
    "wow_change_pct": 4.5,
    "regional_notes": "any notable regional variance",
    "flag": true/false
  }},
  "other_external": "any other notable external factor, or null",
  "no_events": true/false
}}
"""

try:
    response = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=AGENT5_MAX_TOKENS,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{"role": "user", "content": weather_prompt}],
    )

    # Extract text from response
    weather_text = ""
    for block in response.content:
        if hasattr(block, "text"):
            weather_text += block.text

    # Parse JSON from response
    import re
    json_match = re.search(r'\{[\s\S]*\}', weather_text)
    if json_match:
        weather_data = json.loads(json_match.group())
    else:
        weather_data = {"no_events": True, "weather_events": [], "gas_price": {}}

except Exception as e:
    report.add_data_quality_note(f"Claude web search failed: {e}. Weather context unavailable.")
    weather_data = {"no_events": True, "weather_events": [], "gas_price": {}}

# ─────────────────────────────────────────────
# 3. BUILD FLAG REPORT
# ─────────────────────────────────────────────

gas_flag_threshold = THRESHOLDS["gas_price_wow_flag_pct"]

weather_events = weather_data.get("weather_events", [])
gas_data       = weather_data.get("gas_price", {})
no_events      = weather_data.get("no_events", False)

if no_events and not weather_events:
    # Silence rule — one line output
    silence_msg = (
        f"AGENT 5 — WEATHER & EXTERNAL EVENTS\n"
        f"Business Date: {BUSINESS_DATE_STR}\n"
        f"{'─' * 60}\n"
        f"STATUS: GREEN — No active weather events in monitored markets.\n"
    )
    # Still check gas prices
    if gas_data.get("flag") and abs(gas_data.get("wow_change_pct", 0) / 100) > gas_flag_threshold:
        wow_pct = gas_data.get("wow_change_pct", 0)
        direction = "up" if wow_pct > 0 else "down"
        silence_msg += (
            f"GAS PRICE: National avg ${gas_data.get('national_avg_today', 0):.2f}, "
            f"{wow_pct:+.1f}% WoW ({direction}). "
            f"{gas_data.get('regional_notes', '')}\n"
        )
    else:
        silence_msg += f"Gas prices within normal range.\n"

    silence_msg += "─" * 60
    agent_5_output = silence_msg
    print(agent_5_output)
    write_flag_history(spark, TBL_FLAG_HISTORY, 5, report, BUSINESS_DATE_STR)
    dbutils.jobs.taskValues.set(key="output",       value=agent_5_output)
    dbutils.jobs.taskValues.set(key="status_color", value="GREEN")
    dbutils.jobs.taskValues.set(key="weather_events", value=[])
    dbutils.notebook.exit(agent_5_output)

# Active weather events found
affected_markets = []
for event in weather_events:
    market   = event.get("market", "Unknown")
    state    = event.get("state", "")
    etype    = event.get("event_type", "weather event")
    severity = event.get("severity", "moderate")
    impact   = event.get("impact", "")
    duration = event.get("duration", "")
    traffic_est = event.get("traffic_impact_pct_estimate")

    affected_markets.append(f"{market}, {state}")

    flag_severity = "CRITICAL" if severity == "severe" else "WATCH"

    impact_str = impact
    if traffic_est is not None:
        impact_str += f" Estimated traffic impact: {traffic_est:+.0f}%."

    report.add_flag(
        severity=flag_severity,
        title=f"{severity.title()} {etype.title()} — {market}, {state}",
        detail=impact_str + (f" Duration: {duration}." if duration else ""),
        metric=f"Severity: {severity} | Type: {etype}",
        forward_look=(
            "Weather context feeds to Commander for Agent 1 comp and Agent 2 traffic anomaly explanation."
        ),
    )

# Gas price flag
if gas_data:
    wow_pct = (gas_data.get("wow_change_pct") or 0) / 100
    if abs(wow_pct) > gas_flag_threshold:
        direction = "rising" if wow_pct > 0 else "falling"
        report.add_flag(
            severity="WATCH",
            title=f"National Gas Price {direction.title()} >5% WoW",
            detail=(
                f"National avg gas: ${gas_data.get('national_avg_today', 0):.2f} today vs "
                f"${gas_data.get('national_avg_7d_ago', 0):.2f} one week ago "
                f"({wow_pct:+.1%} WoW). "
                f"{gas_data.get('regional_notes', '')}"
            ),
            metric=f"WoW change: {wow_pct:+.1%}",
            forward_look="Gas price moves >5% WoW can shift discretionary spend and trip consolidation behavior.",
        )

# ─────────────────────────────────────────────
# 4. OUTPUT
# ─────────────────────────────────────────────

agent_5_output = report.render()
print(agent_5_output)

write_flag_history(spark, TBL_FLAG_HISTORY, 5, report, BUSINESS_DATE_STR)

dbutils.jobs.taskValues.set(key="output",           value=agent_5_output)
dbutils.jobs.taskValues.set(key="status_color",     value=report.status_color)
dbutils.jobs.taskValues.set(key="critical_count",   value=report.critical_count)
dbutils.jobs.taskValues.set(key="watch_count",      value=report.watch_count)
dbutils.jobs.taskValues.set(key="win_count",        value=report.win_count)
dbutils.jobs.taskValues.set(key="weather_events",   value=affected_markets)
