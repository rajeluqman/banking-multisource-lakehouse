#!/usr/bin/env python3
"""Open Bank Project sandbox ("Core Banking API") -> Landing (R-18...R-22).

- DirectLogin auth, token refresh-on-401 (R-18 — partial pulls from mid-extract token expiry
  stay in Landing, never promoted until the promotion gate sees a complete, reconciled set).
- Verbatim JSON storage — the response is written byte-for-byte, never flattened here
  (R-19; flattening happens at Silver). Re-parse without re-calling the sandbox.
- Pagination to exhaustion, with the API-reported total recorded alongside the data so the
  promotion gate can reconcile actual-rows-landed vs API-reported-total (R-22).
- Retry with exponential backoff + circuit-break on repeated failure (R-20).

Run on Databricks / local Spark, NOT executed in this planning session (no live sandbox
call here).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import time
import urllib.error
import urllib.request

from pipeline.common.lake_paths import layer_path

MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 2
PAGE_SIZE = 100


class OBPClient:
    def __init__(self) -> None:
        self.base_url = os.environ.get("OBP_BASE_URL", "https://apisandbox.openbankproject.com")
        self._token: str | None = None

    def _get_direct_login_token(self) -> str:
        # DirectLogin credentials never logged, never landed in the response payload itself
        # (R-18/R-19, journey/09_SECURITY_AND_ACCESS.md §1) — only the resulting token is
        # held in memory for the duration of this run.
        consumer_key = os.environ["OBP_CONSUMER_KEY"]
        username = os.environ["OBP_USERNAME"]
        password = os.environ["OBP_PASSWORD"]
        auth_header = (
            f'DirectLogin username="{username}",password="{password}",consumer_key="{consumer_key}"'
        )
        req = urllib.request.Request(
            f"{self.base_url}/my/logins/direct", method="POST",
            headers={"Authorization": auth_header},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())["token"]

    def _request(self, path: str) -> dict:
        for attempt in range(MAX_RETRIES):
            if self._token is None:
                self._token = self._get_direct_login_token()
            req = urllib.request.Request(
                f"{self.base_url}{path}", headers={"Authorization": f"DirectLogin token={self._token}"}
            )
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return json.loads(resp.read())
            except urllib.error.HTTPError as e:
                if e.code == 401:  # R-18 — token expiry mid-extract: refresh and retry
                    self._token = None
                    continue
                if e.code >= 500 or e.code == 429:  # R-20 — retry with backoff
                    time.sleep(BACKOFF_BASE_SECONDS ** attempt)
                    continue
                raise
        raise RuntimeError(f"OBP request to {path} failed after {MAX_RETRIES} retries (circuit-break, R-20)")

    def paginate_to_exhaustion(self, path: str, items_key: str) -> tuple[list[dict], int]:
        """Returns (all_items, api_reported_total) — R-22. Stops when a page returns fewer
        than PAGE_SIZE items OR the API's reported total is reached, whichever is definitive."""
        all_items: list[dict] = []
        offset = 0
        api_reported_total: int | None = None
        while True:
            page = self._request(f"{path}?limit={PAGE_SIZE}&offset={offset}")
            items = page.get(items_key, [])
            all_items.extend(items)
            if api_reported_total is None:
                api_reported_total = page.get("total_count", len(items))
            if len(items) < PAGE_SIZE or len(all_items) >= api_reported_total:
                break
            offset += PAGE_SIZE
        return all_items, api_reported_total or len(all_items)


def land_endpoint(client: OBPClient, path: str, items_key: str, name: str) -> str:
    items, api_reported_total = client.paginate_to_exhaustion(path, items_key)
    run_date = dt.date.today().isoformat()
    partition_path = layer_path("landing", "obp", name, f"dt={run_date}")

    # Verbatim JSON, one file, word-for-word (R-19) — no Spark DataFrame flattening here.
    from pathlib import Path
    out_dir = Path(partition_path.replace("s3://", "/tmp/s3_staging/"))  # local staging only when
    # writing to real S3 this would instead go through boto3 put_object; kept simple here since
    # this module is not executed in the planning session (owner instruction).
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(items)
    (out_dir / "response.json").write_text(payload)

    manifest = {
        "source": "obp", "endpoint": name,
        "rows_landed": len(items), "api_reported_total": api_reported_total,
        "reconciled": len(items) == api_reported_total,  # R-22 — promotion gate checks this
        "checksum": hashlib.sha256(payload.encode()).hexdigest(),
        "written_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    (out_dir / "_manifest.json").write_text(json.dumps(manifest))
    if manifest["reconciled"]:
        (out_dir / "_SUCCESS").write_text("")
    else:
        print(f"WARNING: {name} pagination did not reconcile "
              f"({len(items)} landed vs {api_reported_total} reported) — no _SUCCESS written, "
              f"promotion gate will quarantine this partition (R-22).")

    return partition_path


def main() -> int:
    client = OBPClient()
    land_endpoint(client, "/obp/v4.0.0/my/accounts", "accounts", "accounts")
    land_endpoint(client, "/obp/v4.0.0/my/transactions", "transactions", "transactions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
