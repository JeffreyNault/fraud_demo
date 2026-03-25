# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## War Room — Shared Utilities
# MAGIC Helper functions used across all agents and the Commander.

# COMMAND ----------

import json
import textwrap
import requests
from datetime import date, timedelta
from typing import Optional

# ─────────────────────────────────────────────
# FLAG BUILDER
# ─────────────────────────────────────────────

class FlagReport:
    """Accumulates flags for a single agent and renders a structured output string."""

    SEVERITIES = ("CRITICAL", "WATCH", "WIN", "GREEN")

    def __init__(self, agent_id: int, agent_name: str, business_date: str):
        self.agent_id = agent_id
        self.agent_name = agent_name
        self.business_date = business_date
        self.flags: list[dict] = []
        self.data_quality_notes: list[str] = []

    def add_flag(
        self,
        severity: str,
        title: str,
        detail: str,
        metric: Optional[str] = None,
        impact: Optional[str] = None,
        forward_look: Optional[str] = None,
    ):
        assert severity in self.SEVERITIES, f"Unknown severity: {severity}"
        self.flags.append(
            dict(
                severity=severity,
                title=title,
                detail=detail,
                metric=metric,
                impact=impact,
                forward_look=forward_look,
            )
        )

    def add_data_quality_note(self, note: str):
        self.data_quality_notes.append(note)

    def render(self) -> str:
        lines = [
            f"AGENT {self.agent_id} — {self.agent_name.upper()}",
            f"Business Date: {self.business_date}",
            "─" * 60,
        ]

        if not self.flags:
            lines.append("STATUS: GREEN — No exceptions flagged.")
        else:
            for f in self.flags:
                lines.append(f"\n[{f['severity']}]  {f['title']}")
                lines.append(f"  Detail:       {f['detail']}")
                if f["metric"]:
                    lines.append(f"  Metric:       {f['metric']}")
                if f["impact"]:
                    lines.append(f"  Impact:       {f['impact']}")
                if f["forward_look"]:
                    lines.append(f"  Forward look: {f['forward_look']}")

        if self.data_quality_notes:
            lines.append("\nDATA QUALITY NOTES:")
            for n in self.data_quality_notes:
                lines.append(f"  • {n}")

        lines.append("─" * 60)
        return "\n".join(lines)

    @property
    def status_color(self) -> str:
        severities = {f["severity"] for f in self.flags}
        if "CRITICAL" in severities:
            return "RED"
        if "WATCH" in severities:
            return "YELLOW"
        if self.data_quality_notes:
            return "YELLOW"
        return "GREEN"

    @property
    def critical_count(self) -> int:
        return sum(1 for f in self.flags if f["severity"] == "CRITICAL")

    @property
    def watch_count(self) -> int:
        return sum(1 for f in self.flags if f["severity"] == "WATCH")

    @property
    def win_count(self) -> int:
        return sum(1 for f in self.flags if f["severity"] == "WIN")


# ─────────────────────────────────────────────
# DELTA TABLE HELPERS
# ─────────────────────────────────────────────

def write_flag_history(spark, tbl: str, agent_id: int, report: FlagReport, business_date: str):
    """Persist agent flag output to Delta for historical trending."""
    from pyspark.sql import Row
    rows = [
        Row(
            business_date=business_date,
            agent_id=agent_id,
            agent_name=report.agent_name,
            severity=f["severity"],
            flag_title=f["title"],
            flag_detail=f["detail"],
            impact=f.get("impact"),
            created_ts=str(date.today()),
        )
        for f in report.flags
    ]
    if not rows:
        # Write a GREEN sentinel row so history is complete
        rows = [
            Row(
                business_date=business_date,
                agent_id=agent_id,
                agent_name=report.agent_name,
                severity="GREEN",
                flag_title="No exceptions",
                flag_detail="No exceptions flagged.",
                impact=None,
                created_ts=str(date.today()),
            )
        ]
    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("append").saveAsTable(tbl)


def get_prior_week_flag_counts(spark, tbl: str, business_date: str) -> dict:
    """Return flag counts from 7 days ago for trend comparison."""
    from datetime import datetime
    prior = (datetime.strptime(business_date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
    try:
        df = spark.sql(f"""
            SELECT severity, COUNT(*) AS cnt
            FROM {tbl}
            WHERE business_date = '{prior}'
            AND severity != 'GREEN'
            GROUP BY severity
        """)
        rows = {r["severity"]: r["cnt"] for r in df.collect()}
        return {
            "critical": rows.get("CRITICAL", 0),
            "watch":    rows.get("WATCH", 0),
            "win":      rows.get("WIN", 0),
            "total":    sum(rows.values()),
        }
    except Exception:
        return {"critical": 0, "watch": 0, "win": 0, "total": 0}


# ─────────────────────────────────────────────
# NOTIFICATION HELPERS
# ─────────────────────────────────────────────

def post_to_teams(webhook_url: str, briefing_text: str, business_date: str):
    """Post the executive briefing to a Microsoft Teams channel via webhook."""
    payload = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "summary": f"War Room Briefing — {business_date}",
        "themeColor": "D10000",
        "sections": [
            {
                "activityTitle": f"📊 Daily War Room Briefing — {business_date}",
                "activityText": f"```\n{briefing_text[:3500]}\n```",
            }
        ],
    }
    resp = requests.post(webhook_url, json=payload, timeout=10)
    resp.raise_for_status()
    print("Teams notification sent.")


def send_email(smtp_config: dict, recipients: list[str], subject: str, body: str):
    """Send briefing via SMTP email."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    msg = MIMEMultipart()
    msg["From"]    = smtp_config["from_address"]
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP(smtp_config["host"], smtp_config.get("port", 587)) as server:
        server.starttls()
        server.login(smtp_config["user"], smtp_config["password"])
        server.sendmail(smtp_config["from_address"], recipients, msg.as_string())
    print(f"Email sent to {len(recipients)} recipients.")


# ─────────────────────────────────────────────
# ANTHROPIC CLIENT FACTORY
# ─────────────────────────────────────────────

def get_anthropic_client(api_key: str):
    """Return an Anthropic client instance."""
    import anthropic
    return anthropic.Anthropic(api_key=api_key)


print("War Room Utils loaded.")
