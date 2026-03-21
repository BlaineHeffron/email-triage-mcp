# Email Triage MCP

Rust MCP server that classifies inbound emails with a background terminal model command and can optionally route the result to any webhook-style API.

## Fit

Yes, this should be an MCP server if your goal is to expose email triage as a reusable tool to Codex, Claude Desktop, or any other MCP client.

A plugin is only the better fit if you need a host-specific integration surface tied to one product's auth and manifest model. For a local or self-hosted email workflow, MCP is the cleaner architecture.

## What It Does

- exposes `triage_email`
- exposes `route_email`
- exposes `build_generic_connector`
- exposes `list_connector_defaults`
- exposes `build_connector_flow`
- exposes `run_connector_flow`
- exposes `configure_triage_flow`
- exposes `enqueue_email_batch`
- exposes `process_queue`
- exposes `get_queue_status`
- runs classification by shelling out to `CLASSIFIER_COMMAND`
- sends optional routing payloads to any HTTP endpoint
- exposes generic `imap` and `webhook` transports plus named presets such as `gmail`, `joplin`, and `highlevel`

## Connector Model

The intended UX is that the user describes the workflow in plain English and the MCP client turns that into a flow config.

Examples:

- "Take unread Gmail messages and push support leads into my CRM"
- "Read IMAP mail from billing@ and send triage payloads to this webhook"
- "Use Gmail as the source and create Joplin notes for anything urgent"

The server is structured around:

- generic transports: `imap`, `webhook`
- named presets: `gmail`, `joplin`, `crm`, `highlevel`
- overrides: folder names, endpoints, notebook IDs, env var names, route fallbacks

That keeps the built-in connector surface small while still giving the AI enough structure to assemble flows reliably.

## Classifier Model

Point `CLASSIFIER_COMMAND` at any CLI that reads from stdin and returns strict JSON on stdout.

Examples:

```bash
export CLASSIFIER_COMMAND='claude -p'
```

```bash
export CLASSIFIER_COMMAND='codex exec --json'
```

If your preferred CLI needs flags, session setup, or prompt wrapping, use a shell script and point `CLASSIFIER_COMMAND` at that script instead.

## Expected JSON From The Classifier

```json
{
  "category": "support",
  "confidence": 0.94,
  "summary": "Customer is reporting a failed login after password reset.",
  "reasoning": "The email is requesting account help.",
  "suggestedRoute": "helpdesk.auth",
  "priority": "high",
  "suggestedNextStep": "Create a support task and respond with password reset troubleshooting.",
  "actionItems": [
    "Open a support ticket",
    "Assign to the authentication queue"
  ],
  "contactHints": ["Existing customer", "Needs response today"],
  "tags": ["auth", "login"]
}
```

Allowed categories are `sales`, `support`, `billing`, `spam`, `personal`, `urgent`, and `other`.
Allowed priorities are `low`, `medium`, `high`, and `urgent`.

## Run

```bash
cargo build
CLASSIFIER_COMMAND='claude -p' cargo run
```

## Gmail To Joplin Test

Set these first:

```bash
export CLASSIFIER_COMMAND='claude -p'
export GMAIL_EMAIL='you@gmail.com'
export GMAIL_APP_PASSWORD='your-app-password'
export JOPLIN_TOKEN='your-joplin-token'
```

Joplin should be running locally with the Web Clipper API enabled. The default endpoint is `http://127.0.0.1:41184`.

Then build a modular flow with defaults:

```json
{
  "source": {
    "kind": "imap",
    "preset": "gmail",
    "host": "imap.gmail.com",
    "port": 993,
    "folder": "INBOX",
    "unreadOnly": true,
    "maxEmails": 1,
    "passwordEnv": "GMAIL_APP_PASSWORD"
  },
  "destination": {
    "kind": "joplin",
    "endpoint": "http://127.0.0.1:41184",
    "tokenEnv": "JOPLIN_TOKEN",
    "tag": "email-triage"
  }
}
```

Use that object with `run_connector_flow`. If you want a safe first pass, set `dryRun` to `true`.

The design goal here is that a client can say "source is gmail, destination is joplin" and the server fills in sane defaults, while still allowing overrides for folders, notebook IDs, endpoints, and tags.

## Gmail To HighLevel Example

```json
{
  "source": {
    "kind": "imap",
    "preset": "gmail",
    "passwordEnv": "GMAIL_APP_PASSWORD"
  },
  "destination": {
    "kind": "webhook",
    "preset": "highlevel",
    "endpoint": "https://services.leadconnectorhq.com/email-triage",
    "tokenEnv": "HIGHLEVEL_API_KEY"
  },
  "routeUnclassifiedTo": "crm.untriaged"
}
```

This is deliberately still a webhook transport underneath. If the user says "send it to my HighLevel CRM with action items", the AI should treat HighLevel as a preset starting point, then fill endpoint details and payload expectations via overrides.

## Queue Mode

For production use, prefer the queue-oriented flow instead of a single large `run_connector_flow` call:

1. `build_connector_flow`
2. `configure_triage_flow`
3. `enqueue_email_batch`
4. `process_queue`
5. `get_queue_status`

This keeps MCP as the control plane while the server processes email jobs sequentially as a worker.

## MCP Client Example

```json
{
  "mcpServers": {
    "email-triage": {
      "command": "/absolute/path/to/email-triage-mcp/target/debug/email-triage-mcp",
      "env": {
        "CLASSIFIER_COMMAND": "claude -p"
      }
    }
  }
}
```

## Next Step

If you want this to ingest real mail, add a separate worker in front of it for Gmail, IMAP, SES, or Postmark. Keep that ingestion layer outside the MCP server so the MCP surface stays simple: classify, route, and return structured output.
