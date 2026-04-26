# Databricks notebook source
# Triggers the GitHub Actions workflow that uploads the latest dashboard assets
# from the UC Volume to Cloudflare R2.
#
# Requires the following Databricks secrets (default scope: github,
# override with the `secret_scope` job parameter):
#   github_token       — PAT or fine-grained token with `actions:write` on the repo
#   github_repo        — "owner/repo" (e.g. "richmulvany/sc-warcraftlogs-analytics")
#   github_workflow    — workflow file name (default: "publish-dashboard-data.yml")
#   github_ref         — git ref to dispatch on (default: "main")
#
# Secrets-not-set fall back to the corresponding job parameters where useful.

# COMMAND ----------
import json
import time
import urllib.error
import urllib.request

dbutils.widgets.text("secret_scope", "github")  # noqa: F821
dbutils.widgets.text("github_repo", "")                   # noqa: F821
dbutils.widgets.text("github_workflow", "publish-dashboard-data.yml")  # noqa: F821
dbutils.widgets.text("github_ref", "main")                # noqa: F821
dbutils.widgets.text("poll_seconds", "600")               # noqa: F821

scope    = dbutils.widgets.get("secret_scope")             # noqa: F821
workflow = dbutils.widgets.get("github_workflow") or "publish-dashboard-data.yml"
ref      = dbutils.widgets.get("github_ref") or "main"
poll_s   = int(dbutils.widgets.get("poll_seconds") or "600")


def _secret(key: str, default: str = "") -> str:
    try:
        return dbutils.secrets.get(scope=scope, key=key)  # noqa: F821
    except Exception:
        return default


token = _secret("github_token")
if not token:
    raise RuntimeError(
        f"Missing secret '{scope}/github_token'. Create it with: "
        f"databricks secrets put-secret {scope} github_token"
    )

repo = dbutils.widgets.get("github_repo") or _secret("github_repo")  # noqa: F821
if not repo or "/" not in repo:
    raise RuntimeError(
        "github_repo must be set as a job parameter or secret in 'owner/repo' form"
    )

api = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches"
print(f"Dispatching {workflow} on {repo}@{ref} via {api}")

body = json.dumps({"ref": ref}).encode("utf-8")
req = urllib.request.Request(
    api,
    data=body,
    method="POST",
    headers={
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
        "User-Agent": "sc-analytics-databricks-orchestrator",
    },
)
dispatched_at = time.time()
try:
    with urllib.request.urlopen(req, timeout=30) as resp:
        if resp.status not in (201, 204):
            raise RuntimeError(f"workflow_dispatch returned HTTP {resp.status}")
except urllib.error.HTTPError as e:
    raise RuntimeError(f"workflow_dispatch failed: HTTP {e.code} — {e.read().decode('utf-8', 'replace')}") from e

print("workflow_dispatch accepted; polling for the matching run …")

# COMMAND ----------
# Poll for the run we just dispatched (the API does not return a run id directly).
runs_api = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/runs?event=workflow_dispatch&branch={ref}&per_page=10"
deadline = dispatched_at + poll_s
run = None
while time.time() < deadline:
    req = urllib.request.Request(
        runs_api,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "sc-analytics-databricks-orchestrator",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read())
    for candidate in payload.get("workflow_runs") or []:
        created_at = candidate.get("created_at", "")
        try:
            created_ts = time.mktime(time.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ"))
        except ValueError:
            continue
        if created_ts >= dispatched_at - 60:
            run = candidate
            break
    if run and run.get("status") == "completed":
        break
    time.sleep(15)

if run is None:
    raise RuntimeError(
        f"No matching workflow_dispatch run appeared within {poll_s}s — check GitHub Actions UI"
    )

conclusion = run.get("conclusion")
print(f"Run {run.get('id')} status={run.get('status')} conclusion={conclusion} url={run.get('html_url')}")
if run.get("status") != "completed":
    raise RuntimeError(
        f"GitHub workflow run {run.get('id')} did not complete within {poll_s}s "
        f"(status={run.get('status')}); see {run.get('html_url')}"
    )
if conclusion != "success":
    raise RuntimeError(f"GitHub workflow run concluded with '{conclusion}': {run.get('html_url')}")

print("GitHub Actions publish workflow completed successfully.")
