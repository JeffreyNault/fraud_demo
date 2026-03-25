# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## War Room Commander — Synthesis Engine
# MAGIC
# MAGIC Runs after all 8 agents complete. Collects structured flag reports via task values,
# MAGIC calls Claude (claude-sonnet-4-6) to synthesize the executive briefing, stores the
# MAGIC result in Delta, and delivers to Teams + email.
# MAGIC
# MAGIC **Input:** Agent task values (output strings + metadata)
# MAGIC **Output:** Single executive briefing — DAILY WAR ROOM BRIEFING

# COMMAND ----------

# MAGIC %run ../shared/00_config

# COMMAND ----------

# MAGIC %run ../shared/00_utils

# COMMAND ----------

import anthropic
import json
from datetime import date, timedelta

# ─────────────────────────────────────────────
# 1. COLLECT AGENT OUTPUTS FROM TASK VALUES
# ─────────────────────────────────────────────

def get_agent_output(task_key: str, default: str = "AGENT: DATA UNAVAILABLE") -> str:
    try:
        return dbutils.jobs.taskValues.get(
            taskKey=task_key, key="output", debugValue=default
        )
    except Exception:
        return default

def get_agent_color(task_key: str) -> str:
    try:
        return dbutils.jobs.taskValues.get(
            taskKey=task_key, key="status_color", debugValue="YELLOW"
        )
    except Exception:
        return "YELLOW"

def get_task_value(task_key: str, key: str, default=None):
    try:
        return dbutils.jobs.taskValues.get(taskKey=task_key, key=key, debugValue=default)
    except Exception:
        return default

# Agent outputs
agent_outputs = {
    1: get_agent_output("agent_01_store_ops"),
    2: get_agent_output("agent_02_customer"),
    3: get_agent_output("agent_03_competitors"),
    4: get_agent_output("agent_04_merch_inventory"),
    5: get_agent_output("agent_05_weather_external"),
    6: get_agent_output("agent_06_digital_ecommerce"),
    7: get_agent_output("agent_07_finance"),
    8: get_agent_output("agent_08_supply_chain"),
}

# Agent colors for scorecard
agent_colors = {
    1: get_agent_color("agent_01_store_ops"),
    2: get_agent_color("agent_02_customer"),
    3: get_agent_color("agent_03_competitors"),
    4: get_agent_color("agent_04_merch_inventory"),
    5: get_agent_color("agent_05_weather_external"),
    6: get_agent_color("agent_06_digital_ecommerce"),
    7: get_agent_color("agent_07_finance"),
    8: get_agent_color("agent_08_supply_chain"),
}

# Agent 3 vintage date
competitor_vintage = get_task_value("agent_03_competitors", "data_vintage", "UNKNOWN")

# Cross-agent metadata for synthesis hints
promo_oos_count    = get_task_value("agent_04_merch_inventory", "promo_oos_count", 0)
promo_fill_fail    = get_task_value("agent_08_supply_chain", "promo_fill_failures", 0)
flagged_osa_cats   = get_task_value("agent_04_merch_inventory", "flagged_osa_cats", [])
flagged_nps_cats   = get_task_value("agent_02_customer", "flagged_nps_cats", [])
margin_flagged_cats = get_task_value("agent_07_finance", "margin_flagged_cats", [])
total_gm_delta     = get_task_value("agent_07_finance", "total_gm_delta", 0.0)
company_bps_vs_ly  = get_task_value("agent_07_finance", "company_bps_vs_ly", 0.0)
digital_rev_yoy    = get_task_value("agent_06_digital_ecommerce", "digital_rev_yoy", 0.0)
weather_markets    = get_task_value("agent_05_weather_external", "weather_events", [])
promo_risk_fwd     = get_task_value("agent_08_supply_chain", "promo_risk_forward", 0)

print("Agent outputs collected.")
print(f"Agent status colors: {agent_colors}")

# ─────────────────────────────────────────────
# 2. PRIOR WEEK FLAG COUNTS (TREND)
# ─────────────────────────────────────────────

prior_counts = get_prior_week_flag_counts(spark, TBL_FLAG_HISTORY, BUSINESS_DATE_STR)

# Today's flag counts (summed across all agents)
today_counts = {}
for agent_task in [
    "agent_01_store_ops", "agent_02_customer", "agent_03_competitors",
    "agent_04_merch_inventory", "agent_05_weather_external", "agent_06_digital_ecommerce",
    "agent_07_finance", "agent_08_supply_chain"
]:
    today_counts["critical"] = today_counts.get("critical", 0) + (get_task_value(agent_task, "critical_count", 0) or 0)
    today_counts["watch"]    = today_counts.get("watch", 0)    + (get_task_value(agent_task, "watch_count", 0) or 0)
    today_counts["win"]      = today_counts.get("win", 0)      + (get_task_value(agent_task, "win_count", 0) or 0)

today_counts["total"] = today_counts["critical"] + today_counts["watch"] + today_counts["win"]

# ─────────────────────────────────────────────
# 3. BUILD COMMANDER SYSTEM PROMPT
# ─────────────────────────────────────────────

COMMANDER_SYSTEM_PROMPT = """You are the War Room Commander for a major regional supercenter retailer.
Your job is to synthesize the daily flag reports from 8 domain agents into a
single executive briefing for the CEO, CFO, and senior leadership team.

You do not fetch or analyze raw data. You work only from agent reports.

ROLE
You are the most trusted senior advisor in the room. You speak plainly,
directly, and without spin. You deliver bad news clearly. You do not soften
or hedge findings. You do not congratulate the business on stable metrics.

PRIORITY ORDER
When multiple flags compete for attention, prioritize in this order:
  1. Comp sales vs. LY
  2. Customer traffic trends
  3. Operational risk (supply chain, OOS, fulfillment)
  4. Competitive threat
  5. Gross margin impact

SYNTHESIS RULES
- If multiple agent flags share a root cause, treat them as ONE issue.
  Name the root cause explicitly and list which agents flagged it.
- Always explain WHY a flag matters, not just what happened.
- Quantify dollar or % impact only when it is material and clear.
- Never assign owners. Flag the issue and let leadership decide.
- Never reproduce agent flags verbatim. Synthesize and rewrite.
- If an agent reported GREEN / no flags, do not mention that domain.

GLOBAL RULES (ALWAYS ENFORCE)
- Tobacco and Gift Cards are excluded from all analysis.
- Fuel and Pharmacy are flagged independently only if agents surfaced them.
- Never benchmark Supercenter vs. Grocery vs. Market format stores.
- Agent 2 has transaction-level NPS and satisfaction scores by survey category
  (e.g. checkout, produce, cleanliness) and channel (in-store vs. digital).
  Use category-level satisfaction declines to explain comp or OOS flags
  from Agents 1 and 4. Only cite scores where sample size is sufficient.
- Agent 5 (Weather) provides context only — not a standalone escalation
  unless a severe event is directly impacting operations.

CROSS-AGENT SYNTHESIS PATTERNS
- Supply Chain → Inventory → Comp Sales (Agents 8 + 4 + 1):
  Vendor fill rate miss → promo item OOS → comp sales miss = ONE story.
- Customer Traffic → Comp Decomposition (Agents 2 + 1):
  Household visit decline + comp miss = traffic-driven miss, not basket-driven.
- Category Satisfaction → OOS → Comp Miss (Agents 2 + 4 + 1):
  Satisfaction decline + OOS in same category + comp miss = strongest three-agent signal.
- Weather → Traffic → Comp (Agents 5 + 1 + 2):
  Use weather as root cause only if flagged markets align with underperforming stores.
- Digital Offset to Store Comp (Agents 6 + 1):
  Store comp miss partially offset by digital gain = report total company comp and note channel shift.
- Promo Execution Failure (Agents 4 + 7 + 8):
  Promo OOS + markdown above LY + vendor fill rate miss = promotional execution breakdown.
- Competitive Pressure on Traffic (Agents 3 + 1 + 2):
  Competitor promo activity + traffic decline + comp miss in overlapping markets.

FLAG SEVERITY
  CRITICAL  — Requires executive awareness or decision today.
  WATCH     — Emerging risk. Monitor closely. No action required yet.
  WIN       — Material positive result worth noting. Use sparingly.

OUTPUT FORMAT
Produce output in EXACTLY this structure. No sections may be added, removed, or relabeled:

DAILY WAR ROOM BRIEFING
[DATE]    BUSINESS CONDITION: [ CRITICAL | STRESSED | STABLE | STRONG ]

THE STORY
[Sentence 1: What happened yesterday — the headline comp and traffic result.]
[Sentence 2: Why it matters — root cause or business implication.]
[Sentence 3: What leadership needs to know or act on right now.]

FLAG TREND
Today:  [N] flags  (Critical: [N]  |  Watch: [N]  |  Win: [N])
Prior week:  [N] flags  (Critical: [N]  |  Watch: [N]  |  Win: [N])
Direction:  Improving ↑  /  Deteriorating ↓  /  Stable →

AGENT SCORECARD
Store Ops | Customer | Competitors | Merch/Inv | Weather | Digital | Finance | Supply Chain
[COLOR]   | [COLOR]  | [COLOR]     | [COLOR]   | [COLOR] | [COLOR] | [COLOR] | [COLOR]

CRITICAL FLAGS — ACTION REQUIRED TODAY

FLAG 1:  [Flag title]
Root cause:  [What drove this. Connect agents explicitly if shared root cause.]
Agents:  [Agent X, Agent Y]
Impact:  [$X or X% — only if material and clear. Otherwise omit.]
Forward look:  [What happens next if unaddressed.]

[Additional flags...]

WATCH LIST — MONITOR CLOSELY

WATCH 1:  [Watch item title]
Why:  [What to watch and over what timeframe.]

[Additional watch items — maximum 3...]

WINS — MATERIAL POSITIVES ONLY

WIN 1:  [Win title — only if genuinely material. Omit section entirely if none.]
Why it matters:  [Business context.]
"""

# ─────────────────────────────────────────────
# 4. BUILD COMMANDER USER PROMPT
# ─────────────────────────────────────────────

# Scorecard line for the user prompt (to help Commander fill in accurately)
scorecard_hint = "  |  ".join(
    f"Agent {n}: {agent_colors[n]}" for n in range(1, 9)
)

# Cross-agent pattern hints (helps Commander spot synthesis opportunities)
cross_agent_hints = []
if promo_oos_count > 0 and promo_fill_fail > 0:
    cross_agent_hints.append(
        f"SYNTHESIS HINT: {promo_fill_fail} promo vendor fill failures (Agent 8) "
        f"align with {promo_oos_count} promo OOS combos (Agent 4) — "
        "promo execution failure chain likely."
    )
cats_both = set(flagged_osa_cats) & set(flagged_nps_cats)
if cats_both:
    cross_agent_hints.append(
        f"SYNTHESIS HINT: Categories flagged for both OOS (Agent 4) AND "
        f"customer satisfaction decline (Agent 2): {', '.join(cats_both)}. "
        "This is the strongest three-agent signal — connect to Agent 1 comp if available."
    )
if digital_rev_yoy and digital_rev_yoy > 0.03:
    cross_agent_hints.append(
        f"SYNTHESIS HINT: Digital revenue +{digital_rev_yoy:.1%} YoY (Agent 6) "
        "may partially offset store comp miss (Agent 1) — report total company comp picture."
    )
if weather_markets:
    cross_agent_hints.append(
        f"SYNTHESIS HINT: Active weather events in {len(weather_markets)} markets (Agent 5). "
        "Check if these markets align with store underperformers in Agent 1 and traffic miss in Agent 2."
    )

cross_hints_text = "\n".join(cross_agent_hints) if cross_agent_hints else "(No cross-agent patterns pre-detected — synthesize from agent outputs.)"

commander_user_prompt = f"""Today is {BUSINESS_DATE_STR}. Below are the flag reports from all 8 domain agents.
Produce the Daily Executive War Room Briefing following your output template.

Prior week flag count: {prior_counts['total']} flags  (Critical: {prior_counts['critical']} | Watch: {prior_counts['watch']} | Win: {prior_counts['win']})

Current day totals for your FLAG TREND section: {today_counts['total']} flags (Critical: {today_counts['critical']} | Watch: {today_counts['watch']} | Win: {today_counts['win']})

AGENT SCORECARD COLORS (use these exactly):
{scorecard_hint}

CROSS-AGENT SYNTHESIS OPPORTUNITIES:
{cross_hints_text}

─────────────────────────────────────────────
AGENT 1 — STORE OPERATIONS
─────────────────────────────────────────────
{agent_outputs[1]}

─────────────────────────────────────────────
AGENT 2 — CUSTOMER
─────────────────────────────────────────────
{agent_outputs[2]}

─────────────────────────────────────────────
AGENT 3 — COMPETITORS  [Data vintage: {competitor_vintage}]
─────────────────────────────────────────────
{agent_outputs[3]}

─────────────────────────────────────────────
AGENT 4 — MERCHANDISING & INVENTORY
─────────────────────────────────────────────
{agent_outputs[4]}

─────────────────────────────────────────────
AGENT 5 — WEATHER & EXTERNAL EVENTS
─────────────────────────────────────────────
{agent_outputs[5]}

─────────────────────────────────────────────
AGENT 6 — DIGITAL & E-COMMERCE
─────────────────────────────────────────────
{agent_outputs[6]}

─────────────────────────────────────────────
AGENT 7 — FINANCE
─────────────────────────────────────────────
{agent_outputs[7]}

─────────────────────────────────────────────
AGENT 8 — SUPPLY CHAIN & LOGISTICS
─────────────────────────────────────────────
{agent_outputs[8]}
"""

print("Commander prompt assembled.")
print(f"Prompt length: {len(commander_user_prompt):,} characters")

# ─────────────────────────────────────────────
# 5. CALL CLAUDE — SYNTHESIZE BRIEFING
# ─────────────────────────────────────────────

client = get_anthropic_client(ANTHROPIC_API_KEY)

try:
    response = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=COMMANDER_MAX_TOKENS,
        system=COMMANDER_SYSTEM_PROMPT,
        messages=[
            {"role": "user", "content": commander_user_prompt}
        ],
    )
    briefing = response.content[0].text
    print("Commander briefing generated successfully.")

except anthropic.APIError as e:
    briefing = (
        f"DAILY WAR ROOM BRIEFING\n"
        f"{BUSINESS_DATE_STR}    BUSINESS CONDITION: DATA UNAVAILABLE\n\n"
        f"THE STORY\n"
        f"Commander synthesis failed due to API error: {e}\n"
        f"Raw agent outputs are available in the War Room flag_history Delta table.\n"
        f"Manual review of agent outputs required.\n\n"
        f"FLAG TREND\n"
        f"Today: {today_counts['total']} flags (Critical: {today_counts['critical']} | Watch: {today_counts['watch']} | Win: {today_counts['win']})\n"
        f"Prior week: {prior_counts['total']} flags\n"
    )

print("\n" + "=" * 80)
print(briefing)
print("=" * 80)

# ─────────────────────────────────────────────
# 6. PERSIST BRIEFING TO DELTA
# ─────────────────────────────────────────────

from pyspark.sql import Row

briefing_row = Row(
    business_date    = BUSINESS_DATE_STR,
    briefing_text    = briefing,
    total_flags      = today_counts["total"],
    critical_count   = today_counts["critical"],
    watch_count      = today_counts["watch"],
    win_count        = today_counts["win"],
    agent_colors     = json.dumps(agent_colors),
    created_ts       = str(date.today()),
)

briefing_df = spark.createDataFrame([briefing_row])
briefing_df.write.format("delta").mode("append").saveAsTable(TBL_BRIEFING_HISTORY)
print(f"Briefing persisted to {TBL_BRIEFING_HISTORY}.")

# ─────────────────────────────────────────────
# 7. DELIVER — TEAMS + EMAIL
# ─────────────────────────────────────────────

# Teams
try:
    post_to_teams(TEAMS_WEBHOOK_URL, briefing, BUSINESS_DATE_STR)
except Exception as e:
    print(f"Teams delivery failed: {e}")

# Email
try:
    recipients = [addr.strip() for addr in EMAIL_DISTRIBUTION.split(",")]
    business_condition = "UNKNOWN"
    for line in briefing.split("\n"):
        if "BUSINESS CONDITION" in line:
            for cond in ["CRITICAL", "STRESSED", "STABLE", "STRONG"]:
                if cond in line:
                    business_condition = cond
                    break
            break

    send_email(
        smtp_config={
            "host":         dbutils.secrets.get(scope="war-room", key="smtp-host"),
            "port":         int(dbutils.secrets.get(scope="war-room", key="smtp-port")),
            "user":         dbutils.secrets.get(scope="war-room", key="smtp-user"),
            "password":     dbutils.secrets.get(scope="war-room", key="smtp-password"),
            "from_address": dbutils.secrets.get(scope="war-room", key="smtp-from"),
        },
        recipients=recipients,
        subject=f"[{business_condition}] Daily War Room Briefing — {BUSINESS_DATE_STR}",
        body=briefing,
    )
except Exception as e:
    print(f"Email delivery failed: {e}")

# ─────────────────────────────────────────────
# 8. DISPLAY FINAL OUTPUT
# ─────────────────────────────────────────────

displayHTML(f"<pre style='font-family:monospace;font-size:13px;'>{briefing}</pre>")

dbutils.jobs.taskValues.set(key="briefing", value=briefing)
dbutils.jobs.taskValues.set(key="business_condition", value=business_condition)
