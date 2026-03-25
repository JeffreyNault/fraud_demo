# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Agent 5 — Weather & External Events
# MAGIC **Domain:** Severe Weather, Precipitation, Gas Prices
# MAGIC
# MAGIC - Data source: Bing Search API (live) + Azure OpenAI for summarization
# MAGIC - Market list: Pulled from dimStore at runtime
# MAGIC - Silence rule: If no active weather events, output one line and stop
# MAGIC - Gas price: Flag moves >5% WoW vs 30-day trend

# COMMAND ----------

# MAGIC %run ../shared/00_config

# COMMAND ----------

# MAGIC %run ../shared/00_utils

# COMMAND ----------

import json
import re
import requests as _requests

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
# 2. BING SEARCH — FETCH LIVE WEATHER & GAS DATA
# ─────────────────────────────────────────────

def bing_search(query: str, count: int = 5) -> str:
    """Run a Bing Web Search query and return concatenated snippet text."""
    headers = {"Ocp-Apim-Subscription-Key": BING_SEARCH_API_KEY}
    params  = {"q": query, "count": count, "mkt": "en-US", "safeSearch": "Off"}
    resp = _requests.get(BING_SEARCH_ENDPOINT, headers=headers, params=params, timeout=10)
    resp.raise_for_status()
    results = resp.json().get("webPages", {}).get("value", [])
    return "\n".join(
        f"[{r.get('name', '')}] {r.get('snippet', '')}"
        for r in results
    )

search_results = {}
search_errors  = []

try:
    # Severe weather across all markets — one broad query
    market_states = list({r["state_cd"] for r in markets})
    states_str    = " OR ".join(market_states[:10])   # cap query length
    search_results["weather"] = bing_search(
        f"severe weather warning advisory {states_str} {BUSINESS_DATE_STR}", count=8
    )
except Exception as e:
    search_errors.append(f"Weather search failed: {e}")
    search_results["weather"] = ""

try:
    search_results["gas"] = bing_search(
        f"national average gas price today {BUSINESS_DATE_STR} AAA GasBuddy weekly change", count=4
    )
except Exception as e:
    search_errors.append(f"Gas price search failed: {e}")
    search_results["gas"] = ""

# ─────────────────────────────────────────────
# 3. AZURE OPENAI — PARSE SEARCH RESULTS INTO STRUCTURED DATA
# ─────────────────────────────────────────────

client = get_openai_client(AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY, AZURE_OPENAI_API_VERSION)

weather_prompt = f"""You are an analyst for a major regional US retailer. Today is {BUSINESS_DATE_STR}.

Using only the web search results below, identify any ACTIVE weather events or gas price changes
that would materially impact customer traffic at retail stores in these markets:

{market_text}

WEB SEARCH RESULTS — WEATHER:
{search_results['weather']}

WEB SEARCH RESULTS — GAS PRICES:
{search_results['gas']}

INSTRUCTIONS:
1. Report ONLY markets from the list above that have material weather events TODAY or in the next 48 hours.
2. Extract national average gas price today vs. 7 days ago if present. Flag if WoW change exceeds 5%.
3. If no material weather events appear for any listed market, set no_events to true.

Respond with ONLY valid JSON in this exact format:
{{
  "weather_events": [
    {{
      "market": "market name",
      "state": "state abbreviation",
      "event_type": "blizzard|storm|flood|ice|tornado|other",
      "severity": "severe|moderate|minor",
      "impact": "brief description of expected traffic impact",
      "duration": "when it starts/ends or empty string",
      "traffic_impact_pct_estimate": null
    }}
  ],
  "gas_price": {{
    "national_avg_today": 0.0,
    "national_avg_7d_ago": 0.0,
    "wow_change_pct": 0.0,
    "regional_notes": "",
    "flag": false
  }},
  "other_external": null,
  "no_events": true
}}
"""

try:
    response = client.chat.completions.create(
        model=AGENT5_DEPLOYMENT,
        max_tokens=AGENT5_MAX_TOKENS,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": "You are a retail operations analyst. Respond only with valid JSON.",
            },
            {"role": "user", "content": weather_prompt},
        ],
    )
    weather_data = json.loads(response.choices[0].message.content)

except Exception as e:
    for err in search_errors:
        report.add_data_quality_note(err)
    report.add_data_quality_note(f"OpenAI weather parsing failed: {e}. Weather context unavailable.")
    weather_data = {"no_events": True, "weather_events": [], "gas_price": {}}

if search_errors:
    for err in search_errors:
        report.add_data_quality_note(err)

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
