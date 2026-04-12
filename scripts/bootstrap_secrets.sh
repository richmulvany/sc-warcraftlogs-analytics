#!/usr/bin/env bash
# Bootstrap Databricks Secret Scope with project secrets.
# Run once during initial setup: bash scripts/bootstrap_secrets.sh

set -euo pipefail

SCOPE="pipeline-secrets"

echo ""
echo "Bootstrapping Databricks Secret Scope: $SCOPE"
echo ""

if databricks secrets list-scopes | grep -q "$SCOPE"; then
    echo "[OK] Secret scope '$SCOPE' already exists."
else
    databricks secrets create-scope "$SCOPE"
    echo "[OK] Created secret scope '$SCOPE'."
fi

prompt_secret() {
    local key=$1
    local description=$2
    echo ""
    echo "  $description"
    read -rsp "  Value for '$key': " value
    echo ""
    databricks secrets put-secret "$SCOPE" "$key" --string-value "$value"
    echo "  [OK] Set '$key'"
}

prompt_secret "api-key"      "Source API key (from your API provider)"
prompt_secret "api-base-url" "Source API base URL (e.g. https://your-api.example.com)"

echo ""
echo "Done. Reference secrets in pipeline notebooks with:"
echo "  dbutils.secrets.get(scope='pipeline-secrets', key='api-key')"
echo ""
