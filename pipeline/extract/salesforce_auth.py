"""Salesforce authentication — Client Credentials Flow (ADR-006 Add #2, BUILD_REPORT.md §11).

NOT username-password/ROPC (the flow `journey/07_PIPELINE_SPEC.md` and `.env.example`
originally described) — this org's External Client App model doesn't expose that flow at
all (SOAP login disabled by default; no Username-Password toggle under Settings -> Flow
Enablement). Client Credentials Flow is Salesforce's own recommended replacement for
headless server-to-server integration and is what was live-verified working.

`simple_salesforce`'s built-in `SalesforceLogin`/`Salesforce.__init__` do not implement the
`grant_type=client_credentials` token request (only `password` and JWT-bearer grants), so
this module does the OAuth token POST directly and hands the resulting session_id/
instance_url to `Salesforce(session_id=..., instance_url=...)` — same object every other
`simple_salesforce` call site expects, just authenticated differently.

Only `SALESFORCE_LOGIN_URL` (the org's My Domain host, e.g.
`https://your-domain.my.salesforce.com`), `SALESFORCE_CLIENT_ID`, and
`SALESFORCE_CLIENT_SECRET` are used. `SALESFORCE_USERNAME`/`PASSWORD`/`SECURITY_TOKEN` are
UNUSED by this flow — left in `.env`/`.env.example` only for reference, not read here.
"""

from __future__ import annotations

import os

import requests
from simple_salesforce import Salesforce

TOKEN_TIMEOUT_SECONDS = 30


def get_salesforce_client() -> Salesforce:
    login_url = os.environ["SALESFORCE_LOGIN_URL"].rstrip("/")
    resp = requests.post(
        f"{login_url}/services/oauth2/token",
        data={
            "grant_type": "client_credentials",
            "client_id": os.environ["SALESFORCE_CLIENT_ID"],
            "client_secret": os.environ["SALESFORCE_CLIENT_SECRET"],
        },  # secrets-scan:allow — built from env vars, not a literal secret
        timeout=TOKEN_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    token = resp.json()
    return Salesforce(instance_url=token["instance_url"], session_id=token["access_token"])
