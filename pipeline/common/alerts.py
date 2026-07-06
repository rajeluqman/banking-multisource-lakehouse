"""Failure alerting — Slack webhook, solo-owner/one-channel (journey/09_SECURITY_AND_ACCESS.md
§9, journey/07_PIPELINE_SPEC.md "Failure handling"). Same pattern as CIL's
`_notify_slack_failure` — one function, called from every fasa's hard-failure path
(promotion-gate quarantine, DQ hard-fail, Gold build failure), not a bespoke alert path per
fasa.
"""

from __future__ import annotations

import json
import os
import urllib.request


def notify_slack_failure(stage: str, detail: str) -> None:
    """Posts a failure message to the Slack webhook in SLACK_WEBHOOK_URL. No-ops (with a
    stderr print, not a silent swallow) if the webhook isn't configured — a missing webhook
    must not crash the pipeline run it's trying to report on."""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print(f"[alerts] SLACK_WEBHOOK_URL not set — failure NOT sent to Slack. "
              f"stage={stage} detail={detail}")
        return

    payload = json.dumps({"text": f":rotating_light: *banking-multisource-lakehouse* — {stage} failed\n{detail}"}).encode()
    req = urllib.request.Request(webhook_url, data=payload, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10)
    except OSError as e:  # network failure notifying about a failure — log, don't raise
        print(f"[alerts] failed to post Slack alert: {e}")
