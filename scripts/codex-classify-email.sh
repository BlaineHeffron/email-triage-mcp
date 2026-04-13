#!/usr/bin/env bash
set -euo pipefail

tmpdir="$(mktemp -d)"
provider="${CLASSIFIER_PROVIDER:-codex}"
model="${CLASSIFIER_MODEL:-}"

cleanup() {
  rm -rf "$tmpdir"
}
trap cleanup EXIT

prompt="$tmpdir/prompt.txt"
schema="$tmpdir/schema.json"
instructions="$tmpdir/instructions.txt"
stderr_file="$tmpdir/stderr.txt"
output_file="$tmpdir/output.txt"

cat >"$prompt"

cat >"$instructions" <<'TXT'
You are an email triage classifier.
Read the provided email payload and classify it into exactly one of:
sales, support, billing, spam, personal, urgent, other.

Return JSON only with exactly these keys:
category, confidence, summary, reasoning, suggestedRoute, tags

Rules:
- confidence must be a number between 0 and 1
- suggestedRoute may be null
- tags must be an array of short strings
- no markdown fences
- no explanatory text outside the JSON object
TXT

cat >"$schema" <<'JSON'
{
  "type": "object",
  "properties": {
    "category": { "type": "string" },
    "confidence": { "type": "number" },
    "summary": { "type": "string" },
    "reasoning": { "type": "string" },
    "suggestedRoute": { "type": ["string", "null"] },
    "tags": {
      "type": "array",
      "items": { "type": "string" }
    }
  },
  "required": ["category", "confidence", "summary", "reasoning", "suggestedRoute", "tags"],
  "additionalProperties": false
}
JSON

combined_prompt="$tmpdir/combined.txt"
{
  cat "$instructions"
  printf '\n\nEmail payload:\n'
  cat "$prompt"
} >"$combined_prompt"

run_claude() {
  local selected_model="${model:-sonnet}"
  if ! claude -p \
    --model "$selected_model" \
    --verbose \
    --output-format stream-json \
    --include-partial-messages \
    --strict-mcp-config \
    --tools "" \
    --permission-mode bypassPermissions \
    <"$combined_prompt" \
    >"$output_file" 2>"$stderr_file"; then
    echo "Claude classifier failed." >&2
    cat "$stderr_file" >&2 || true
    if rg -q 'organization does not have access|login again|contact your administrator' "$stderr_file"; then
      echo "Claude auth appears invalid for non-interactive runs. Re-run: claude auth login" >&2
    fi
    return 1
  fi

  python3 - "$output_file" <<'PY'
import json, re, sys
from pathlib import Path

text_parts = []
for line in Path(sys.argv[1]).read_text().splitlines():
    if not line.strip():
        continue
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        continue
    if event.get("type") != "stream_event":
        continue
    inner = event.get("event", {})
    if inner.get("type") != "content_block_delta":
        continue
    delta = inner.get("delta", {})
    if delta.get("type") != "text_delta":
        continue
    chunk = delta.get("text")
    if isinstance(chunk, str) and chunk:
        text_parts.append(chunk)

combined = "".join(text_parts).replace("```json", "").replace("```", "").strip()
match = re.search(r"\{.*\}", combined, re.DOTALL)
if not match:
    raise SystemExit("Claude stream did not contain a JSON object")
payload = json.loads(match.group(0))
print(json.dumps(payload, separators=(",", ":")))
PY
}

run_codex() {
  local selected_model="${model:-gpt-5.4-mini}"
  if ! codex exec \
    --skip-git-repo-check \
    --sandbox read-only \
    --json \
    --model "$selected_model" \
    --output-schema "$schema" \
    - <"$combined_prompt" \
    >"$output_file" 2>"$stderr_file"; then
    echo "Codex classifier failed." >&2
    cat "$stderr_file" >&2 || true
    cat "$output_file" >&2 || true
    if rg -q 'invalid_grant|Invalid refresh token' "$stderr_file"; then
      echo "Codex auth appears stale for non-interactive runs. Re-run: codex login" >&2
    elif rg -q 'invalid_grant|Invalid refresh token' "$output_file"; then
      echo "Codex auth appears stale for non-interactive runs. Re-run: codex login" >&2
    fi
    return 1
  fi

  python3 - "$output_file" <<'PY'
import json, re, sys
from pathlib import Path

raw = Path(sys.argv[1]).read_text()
if "invalid_grant" in raw or "Invalid refresh token" in raw:
    raise SystemExit("Codex auth appears stale for non-interactive runs. Re-run: codex login")

for line in raw.splitlines()[::-1]:
    line = line.strip()
    if not line:
        continue
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        continue
    item = event.get("item")
    if isinstance(item, dict):
        msg = item.get("text")
        if isinstance(msg, str):
            text = msg.strip()
            if text.startswith("{") and text.endswith("}"):
                payload = json.loads(text)
                print(json.dumps(payload, separators=(",", ":")))
                raise SystemExit(0)
    msg = (
        event.get("last_assistant_message")
        or event.get("message")
        or event.get("text")
        or event.get("content")
    )
    if isinstance(msg, str):
        text = msg.strip()
        if text.startswith("{") and text.endswith("}"):
            payload = json.loads(text)
            print(json.dumps(payload, separators=(",", ":")))
            raise SystemExit(0)
raise SystemExit("Codex output did not contain a final JSON object")
PY
}

case "$provider" in
  claude)
    run_claude
    ;;
  codex)
    run_codex
    ;;
  auto)
    if run_claude; then
      exit 0
    fi
    run_codex
    ;;
  *)
    echo "Unsupported CLASSIFIER_PROVIDER: $provider" >&2
    exit 1
    ;;
esac
