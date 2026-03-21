#!/usr/bin/env bash
set -euo pipefail

tmpdir="$(mktemp -d)"
model="${CLASSIFIER_MODEL:-sonnet}"
cleanup() {
  rm -rf "$tmpdir"
}
trap cleanup EXIT

prompt="$tmpdir/prompt.txt"
raw_stream="$tmpdir/stream.jsonl"

cat >"$prompt"

cat >"$tmpdir/instructions.txt" <<'TXT'
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

{
  cat "$tmpdir/instructions.txt"
  printf '\n\n'
  cat "$prompt"
} | claude -p \
  --model "$model" \
  --verbose \
  --output-format stream-json \
  --include-partial-messages \
  --strict-mcp-config \
  --tools "" \
  --permission-mode bypassPermissions \
  >"$raw_stream"

python3 - "$raw_stream" <<'PY'
import json, re
import sys
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

combined = "".join(text_parts)
combined = combined.replace("```json", "").replace("```", "").strip()

match = re.search(r"\{.*\}", combined, re.DOTALL)
if not match:
    raise SystemExit("Claude stream did not contain a JSON object")

payload = json.loads(match.group(0))
required = {"category", "confidence", "summary", "reasoning", "suggestedRoute", "tags"}
missing = required.difference(payload)
if missing:
    raise SystemExit(f"Claude JSON missing keys: {sorted(missing)}")

print(json.dumps(payload, separators=(",", ":")))
PY
