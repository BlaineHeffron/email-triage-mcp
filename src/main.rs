use imap::types::Fetch;
use mailparse::{self, addrparse_header, MailAddr, MailHeader, MailHeaderMap, ParsedMail};
use native_tls::TlsConnector;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::de::Deserializer;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::env;
use std::fmt::{Display, Formatter};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

const LATEST_PROTOCOL_VERSION: &str = "2025-11-25";
const SUPPORTED_PROTOCOL_VERSIONS: &[&str] =
    &[LATEST_PROTOCOL_VERSION, "2025-06-18", "2025-03-26", "2024-11-05", "2024-10-07"];
const SERVER_NAME: &str = "email-triage-mcp";
const SERVER_VERSION: &str = "0.1.0";

#[derive(Debug)]
enum AppError {
    Io(io::Error),
    Json(serde_json::Error),
    Http(reqwest::Error),
    Message(String),
}

enum CallToolError {
    Protocol(AppError),
    Tool(AppError),
}

impl Display for AppError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::Json(error) => write!(f, "{error}"),
            Self::Http(error) => write!(f, "{error}"),
            Self::Message(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for AppError {}

impl From<io::Error> for AppError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for AppError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<reqwest::Error> for AppError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value)
    }
}

impl From<AppError> for CallToolError {
    fn from(value: AppError) -> Self {
        Self::Tool(value)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonRpcRequest {
    jsonrpc: String,
    id: Option<Value>,
    method: String,
    params: Option<Value>,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct EmailInput {
    from: String,
    #[serde(default)]
    to: Vec<String>,
    #[serde(default)]
    cc: Vec<String>,
    subject: String,
    text_body: Option<String>,
    html_body: Option<String>,
    headers: Option<HashMap<String, String>>,
    received_at: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClassificationResult {
    category: String,
    confidence: f64,
    summary: String,
    reasoning: String,
    suggested_route: Option<String>,
    priority: Option<String>,
    suggested_next_step: Option<String>,
    #[serde(default)]
    action_items: Vec<String>,
    #[serde(default)]
    contact_hints: Vec<String>,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TriageResult {
    email: EmailInput,
    classification: ClassificationResult,
    classifier: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RoutingConfig {
    endpoint: String,
    method: Option<String>,
    headers: Option<HashMap<String, String>>,
    include_raw_email: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct SourceConnectorConfig {
    kind: String,
    preset: Option<String>,
    email_address: Option<String>,
    password_env: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    folder: Option<String>,
    unread_only: Option<bool>,
    max_emails: Option<u32>,
    since_date: Option<String>,
    before_date: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct DestinationConnectorConfig {
    kind: String,
    preset: Option<String>,
    endpoint: Option<String>,
    token_env: Option<String>,
    notebook_id: Option<String>,
    tag: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ConnectorFlowConfig {
    source: SourceConnectorConfig,
    destination: DestinationConnectorConfig,
    route_unclassified_to: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FlowRunResult {
    source: String,
    destination: String,
    processed: usize,
    results: Vec<Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct QueueProcessOptions {
    limit: Option<usize>,
    dry_run: Option<bool>,
}

#[derive(Debug, Clone)]
struct QueuedJob {
    id: u64,
    email: EmailInput,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct QueueRunItem {
    job_id: u64,
    email_subject: String,
    success: bool,
    outcome: Value,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ActiveJobStatus {
    job_id: u64,
    email_subject: String,
    stage: String,
    detail: Option<String>,
}

#[derive(Debug, Default)]
struct AppState {
    default_flow: Option<ConnectorFlowConfig>,
    queue: VecDeque<QueuedJob>,
    next_job_id: u64,
    last_results: Vec<QueueRunItem>,
    active_job: Option<ActiveJobStatus>,
}

fn main() {
    if let Err(error) = run() {
        let _ = writeln!(io::stderr(), "{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), AppError> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut reader = BufReader::new(stdin.lock());
    let mut writer = stdout.lock();

    loop {
        let message = match read_message(&mut reader)? {
            Some(message) => message,
            None => break,
        };

        let request: JsonRpcRequest = serde_json::from_slice(&message)?;

        if request.id.is_none() {
            continue;
        }

        let id = request.id.clone().unwrap_or(Value::Null);
        let response = match handle_request(request) {
            Ok(result) => JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: Some(result),
                error: None,
            },
            Err(error) => JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(JsonRpcError {
                    code: -32000,
                    message: error.to_string(),
                }),
            },
        };

        write_message(&mut writer, &response)?;
        writer.flush()?;
    }

    Ok(())
}

fn handle_request(request: JsonRpcRequest) -> Result<Value, AppError> {
    match request.method.as_str() {
        "initialize" => Ok(json!({
            "protocolVersion": negotiated_protocol_version(request.params.as_ref())?,
            "capabilities": {
                "tools": {}
            },
            "serverInfo": {
                "name": SERVER_NAME,
                "version": SERVER_VERSION
            }
        })),
        "notifications/initialized" => Ok(json!({})),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(list_tools()),
        "tools/call" => match call_tool(request.params.unwrap_or(Value::Null)) {
            Ok(result) => Ok(result),
            Err(CallToolError::Tool(error)) => Ok(tool_error_result(error.to_string())),
            Err(CallToolError::Protocol(error)) => Err(error),
        },
        method => Err(AppError::Message(format!("Unsupported method: {method}"))),
    }
}

fn list_tools() -> Value {
    json!({
        "tools": [
            {
                "name": "triage_email",
                "description": "Classify an email using a background CLI model command such as codex or claude.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "email": email_schema()
                    },
                    "required": ["email"]
                }
            },
            {
                "name": "route_email",
                "description": "Classify an email and optionally send the result to a generic connector API.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "email": email_schema(),
                        "routing": routing_schema()
                    },
                    "required": ["email"]
                }
            },
            {
                "name": "build_generic_connector",
                "description": "Build a generic webhook connector config object for downstream routing.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "endpoint": { "type": "string", "format": "uri" },
                        "apiKey": { "type": "string" }
                    },
                    "required": ["endpoint"]
                }
            },
            {
                "name": "list_connector_defaults",
                "description": "List generic connector templates plus named presets that an MCP client can use as AI-friendly defaults.",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            },
            {
                "name": "build_connector_flow",
                "description": "Build a modular source->destination connector flow from generic transports or named presets.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "sourceKind": { "type": "string", "enum": ["imap", "gmail"] },
                        "destinationKind": { "type": "string", "enum": ["joplin", "webhook", "crm", "highlevel"] },
                        "sourceOverrides": { "type": "object" },
                        "destinationOverrides": { "type": "object" },
                        "routeUnclassifiedTo": { "type": "string" }
                    },
                    "required": ["sourceKind", "destinationKind"]
                }
            },
            {
                "name": "run_connector_flow",
                "description": "Fetch emails from a source connector, classify them, and deliver them to a destination connector.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "flow": connector_flow_schema(),
                        "dryRun": { "type": "boolean" }
                    },
                    "required": ["flow"]
                }
            },
            {
                "name": "configure_triage_flow",
                "description": "Store a default connector flow in the server for later queue-based processing.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "flow": connector_flow_schema()
                    },
                    "required": ["flow"]
                }
            },
            {
                "name": "enqueue_email_batch",
                "description": "Fetch emails using a connector flow and enqueue them for sequential processing.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "flow": connector_flow_schema(),
                        "useDefaultFlow": { "type": "boolean" }
                    }
                }
            },
            {
                "name": "process_queue",
                "description": "Process queued email jobs sequentially using the configured or provided flow.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "flow": connector_flow_schema(),
                        "useDefaultFlow": { "type": "boolean" },
                        "limit": { "type": "integer" },
                        "dryRun": { "type": "boolean" }
                    }
                }
            },
            {
                "name": "get_queue_status",
                "description": "Inspect queue depth, configured flow, and recent processing results.",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            }
        ]
    })
}

fn email_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "from": { "type": "string", "format": "email" },
            "to": { "type": "array", "items": { "type": "string", "format": "email" } },
            "cc": { "type": "array", "items": { "type": "string", "format": "email" } },
            "subject": { "type": "string" },
            "textBody": { "type": "string" },
            "htmlBody": { "type": "string" },
            "headers": { "type": "object", "additionalProperties": { "type": "string" } },
            "receivedAt": { "type": "string" }
        },
        "required": ["from", "subject"]
    })
}

fn routing_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "endpoint": { "type": "string", "format": "uri" },
            "method": { "type": "string", "enum": ["POST", "PUT", "PATCH"] },
            "headers": { "type": "object", "additionalProperties": { "type": "string" } },
            "includeRawEmail": { "type": "boolean" }
        },
        "required": ["endpoint"]
    })
}

fn connector_flow_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "source": {
                "type": "object",
                "properties": {
                    "kind": { "type": "string", "enum": ["imap", "gmail"] },
                    "preset": { "type": "string" },
                    "emailAddress": { "type": "string", "format": "email" },
                    "passwordEnv": { "type": "string" },
                    "host": { "type": "string" },
                    "port": { "type": "integer" },
                    "folder": { "type": "string" },
                    "unreadOnly": { "type": "boolean" },
                    "maxEmails": { "type": "integer" },
                    "sinceDate": { "type": "string", "description": "Inclusive lower bound in YYYY-MM-DD format" },
                    "beforeDate": { "type": "string", "description": "Exclusive upper bound in YYYY-MM-DD format" }
                },
                "required": ["kind"]
            },
            "destination": {
                "type": "object",
                "properties": {
                    "kind": { "type": "string", "enum": ["joplin", "webhook", "crm", "highlevel"] },
                    "preset": { "type": "string" },
                    "endpoint": { "type": "string", "format": "uri" },
                    "tokenEnv": { "type": "string" },
                    "notebookId": { "type": "string" },
                    "tag": { "type": "string" }
                },
                "required": ["kind"]
            },
            "routeUnclassifiedTo": { "type": "string" }
        },
        "required": ["source", "destination"]
    })
}

fn call_tool(params: Value) -> Result<Value, CallToolError> {
    let tool_name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| CallToolError::Protocol(AppError::Message("Missing tool name.".to_string())))?;
    let arguments = params.get("arguments").cloned().unwrap_or(Value::Null);

    match tool_name {
        "triage_email" => {
            let email = parse_email_argument(&arguments)?;
            let triage = triage_email(email)?;
            Ok(tool_result(json!(triage)))
        }
        "route_email" => {
            let email = parse_email_argument(&arguments)?;
            let triage = triage_email(email)?;
            let route = arguments
                .get("routing")
                .map(|value| serde_json::from_value::<RoutingConfig>(value.clone()))
                .transpose()
                .map_err(AppError::from)?;

            let result = if let Some(routing) = route {
                let route_result = send_route(&triage, &routing)?;
                json!({
                    "triage": triage,
                    "route": route_result
                })
            } else {
                json!({
                    "triage": triage,
                    "route": Value::Null
                })
            };

            Ok(tool_result(result))
        }
        "build_generic_connector" => {
            let endpoint = arguments
                .get("endpoint")
                .and_then(Value::as_str)
                .ok_or_else(|| AppError::Message("Missing endpoint.".to_string()))?;
            let api_key = arguments.get("apiKey").and_then(Value::as_str);

            let mut headers = serde_json::Map::new();
            if let Some(key) = api_key {
                headers.insert("authorization".to_string(), Value::String(format!("Bearer {key}")));
            }

            let config = json!({
                "endpoint": endpoint,
                "method": "POST",
                "headers": Value::Object(headers),
                "includeRawEmail": true
            });

            Ok(tool_result(config))
        }
        "list_connector_defaults" => Ok(tool_result(list_connector_defaults())),
        "build_connector_flow" => {
            let source_kind = arguments
                .get("sourceKind")
                .and_then(Value::as_str)
                .ok_or_else(|| AppError::Message("Missing sourceKind.".to_string()))?;
            let destination_kind = arguments
                .get("destinationKind")
                .and_then(Value::as_str)
                .ok_or_else(|| AppError::Message("Missing destinationKind.".to_string()))?;
            let source_overrides = arguments
                .get("sourceOverrides")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let destination_overrides = arguments
                .get("destinationOverrides")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let route_unclassified_to = arguments
                .get("routeUnclassifiedTo")
                .and_then(Value::as_str)
                .map(str::to_string);

            let flow = build_connector_flow(
                source_kind,
                destination_kind,
                source_overrides,
                destination_overrides,
                route_unclassified_to,
            )?;
            Ok(tool_result(json!(flow)))
        }
        "run_connector_flow" => {
            let flow = arguments
                .get("flow")
                .cloned()
                .ok_or_else(|| AppError::Message("Missing flow.".to_string()))?;
            let dry_run = arguments
                .get("dryRun")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let flow: ConnectorFlowConfig =
                serde_json::from_value(flow).map_err(AppError::from)?;
            let result = run_connector_flow(flow, dry_run)?;
            Ok(tool_result(json!(result)))
        }
        "configure_triage_flow" => {
            let flow = arguments
                .get("flow")
                .cloned()
                .ok_or_else(|| AppError::Message("Missing flow.".to_string()))?;
            let flow: ConnectorFlowConfig = serde_json::from_value(flow).map_err(AppError::from)?;
            let mut state = app_state()
                .lock()
                .map_err(|_| AppError::Message("App state lock poisoned.".to_string()))?;
            state.default_flow = Some(flow.clone());
            Ok(tool_result(json!({
                "configured": true,
                "flow": flow
            })))
        }
        "enqueue_email_batch" => {
            let flow = resolve_flow_from_arguments(&arguments)?;
            let emails = match flow.source.kind.as_str() {
                "imap" | "gmail" => fetch_imap_messages(&flow.source)?,
                _ => {
                    return Err(AppError::Message(format!(
                        "Unsupported source connector: {}",
                        flow.source.kind
                    ))
                    .into())
                }
            };

            let mut state = app_state()
                .lock()
                .map_err(|_| AppError::Message("App state lock poisoned.".to_string()))?;
            let mut queued = Vec::new();
            for email in emails {
                state.next_job_id += 1;
                let job = QueuedJob {
                    id: state.next_job_id,
                    email: email.clone(),
                };
                queued.push(json!({
                    "jobId": job.id,
                    "subject": job.email.subject,
                    "from": job.email.from
                }));
                state.queue.push_back(job);
            }

            Ok(tool_result(json!({
                "queued": queued.len(),
                "jobs": queued,
                "queueDepth": state.queue.len()
            })))
        }
        "process_queue" => {
            let flow = resolve_flow_from_arguments(&arguments)?;
            let limit = arguments.get("limit").and_then(Value::as_u64).map(|v| v as usize);
            let dry_run = arguments.get("dryRun").and_then(Value::as_bool).unwrap_or(false);
            let result = process_queue_jobs(&flow, limit, dry_run)?;
            Ok(tool_result(json!(result)))
        }
        "get_queue_status" => {
            let state = app_state()
                .lock()
                .map_err(|_| AppError::Message("App state lock poisoned.".to_string()))?;
            let queued_jobs: Vec<_> = state
                .queue
                .iter()
                .map(|job| {
                    json!({
                        "jobId": job.id,
                        "subject": job.email.subject,
                        "from": job.email.from
                    })
                })
                .collect();
            Ok(tool_result(json!({
                "configuredFlow": state.default_flow,
                "queueDepth": state.queue.len(),
                "activeJob": state.active_job,
                "queuedJobs": queued_jobs,
                "recentResults": state.last_results
            })))
        }
        _ => Err(CallToolError::Protocol(AppError::Message(format!(
            "Unknown tool: {tool_name}"
        )))),
    }
}

fn parse_email_argument(arguments: &Value) -> Result<EmailInput, AppError> {
    let email_value = arguments
        .get("email")
        .cloned()
        .ok_or_else(|| AppError::Message("Missing email argument.".to_string()))?;
    Ok(serde_json::from_value(email_value)?)
}

fn app_state() -> &'static Mutex<AppState> {
    static STATE: OnceLock<Mutex<AppState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(AppState::default()))
}

fn resolve_flow_from_arguments(arguments: &Value) -> Result<ConnectorFlowConfig, CallToolError> {
    if let Some(flow) = arguments.get("flow") {
        return serde_json::from_value(flow.clone())
            .map_err(AppError::from)
            .map_err(CallToolError::from);
    }

    let use_default = arguments
        .get("useDefaultFlow")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    if !use_default {
        return Err(AppError::Message("Missing flow.".to_string()).into());
    }

    let state = app_state()
        .lock()
        .map_err(|_| AppError::Message("App state lock poisoned.".to_string()))
        .map_err(CallToolError::from)?;
    state
        .default_flow
        .clone()
        .ok_or_else(|| AppError::Message("No default flow configured.".to_string()).into())
}

fn negotiated_protocol_version(params: Option<&Value>) -> Result<String, AppError> {
    let requested = params.and_then(|params| {
        params
            .get("protocolVersion")
            .or_else(|| params.get("protocol_version"))
            .and_then(Value::as_str)
    });

    Ok(match requested {
        Some(version) if SUPPORTED_PROTOCOL_VERSIONS.contains(&version) => version.to_string(),
        _ => LATEST_PROTOCOL_VERSION.to_string(),
    })
}

fn triage_email(email: EmailInput) -> Result<TriageResult, AppError> {
    if let Some(classification) = heuristic_classification(&email) {
        return Ok(TriageResult {
            email,
            classification,
            classifier: "heuristic-prefilter".to_string(),
        });
    }

    let classifier = env::var("CLASSIFIER_COMMAND").map_err(|_| {
        AppError::Message(
            "CLASSIFIER_COMMAND is not set. Example: claude -p or codex exec --json".to_string(),
        )
    })?;

    let prompt = build_classifier_prompt(&email);
    let output = run_classifier(&classifier, &prompt)?;

    if !output.status.success() {
        return Err(AppError::Message(format!(
            "Classifier exited with status {}. stderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed = parse_classifier_output(&stdout)?;

    Ok(TriageResult {
        email,
        classification: parsed,
        classifier,
    })
}

fn heuristic_classification(email: &EmailInput) -> Option<ClassificationResult> {
    let subject = email.subject.to_lowercase();
    let from = email.from.to_lowercase();
    let text = email.text_body.as_deref().unwrap_or("").to_lowercase();
    let headers = email.headers.as_ref();

    let has_bulk_headers = headers.is_some_and(|headers| {
        headers.keys().any(|key| {
            key.eq_ignore_ascii_case("List-Unsubscribe")
                || key.eq_ignore_ascii_case("List-Unsubscribe-Post")
                || key.eq_ignore_ascii_case("Precedence")
        })
    });

    let marketing_signals = [
        "unsubscribe",
        "coupon",
        "offer",
        "promo",
        "promotion",
        "save big",
        "limited time",
        "newsletter",
        "sale",
    ];
    let marketing_hits = marketing_signals
        .iter()
        .filter(|signal| subject.contains(**signal) || text.contains(**signal))
        .count();

    let bulk_sender = from.contains("marketing@")
        || from.contains("mailer-daemon")
        || from.contains("no-reply")
        || from.contains("noreply");

    if has_bulk_headers && (marketing_hits >= 1 || bulk_sender) {
        return Some(ClassificationResult {
            category: "spam".to_string(),
            confidence: 0.93,
            summary: "Bulk or promotional email detected from mailing-list style headers and marketing language.".to_string(),
            reasoning: "List-style headers plus promotional sender/content indicate non-personal bulk mail.".to_string(),
            suggested_route: Some("bulk-mail".to_string()),
            priority: Some("low".to_string()),
            suggested_next_step: Some("Archive or unsubscribe unless the sender is explicitly trusted.".to_string()),
            action_items: vec!["Ignore promotional content".to_string()],
            contact_hints: vec![],
            tags: vec!["bulk-mail".to_string(), "marketing".to_string()],
        });
    }

    None
}

fn list_connector_defaults() -> Value {
    json!({
        "sources": {
            "imap": {
                "kind": "imap",
                "host": "imap.example.com",
                "port": 993,
                "folder": "INBOX",
                "unreadOnly": true,
                "maxEmails": 1,
                "sinceDate": null,
                "beforeDate": null,
                "emailAddress": "you@example.com",
                "passwordEnv": "IMAP_PASSWORD"
            },
            "gmail": {
                "kind": "imap",
                "preset": "gmail",
                "host": "imap.gmail.com",
                "port": 993,
                "folder": "INBOX",
                "unreadOnly": true,
                "maxEmails": 1,
                "sinceDate": null,
                "beforeDate": null,
                "passwordEnv": "GMAIL_APP_PASSWORD"
            }
        },
        "destinations": {
            "webhook": {
                "kind": "webhook",
                "endpoint": "https://example.com/email-triage",
                "tokenEnv": "WEBHOOK_TOKEN"
            },
            "joplin": {
                "kind": "joplin",
                "preset": "joplin",
                "endpoint": "http://127.0.0.1:41184",
                "tokenEnv": "JOPLIN_TOKEN",
                "tag": "email-triage"
            },
            "crm": {
                "kind": "webhook",
                "preset": "crm",
                "endpoint": "https://services.leadconnectorhq.com/email-triage",
                "tokenEnv": "HIGHLEVEL_API_KEY"
            },
            "highlevel": {
                "kind": "webhook",
                "preset": "highlevel",
                "endpoint": "https://services.leadconnectorhq.com/email-triage",
                "tokenEnv": "HIGHLEVEL_API_KEY"
            }
        }
    })
}

fn build_connector_flow(
    source_kind: &str,
    destination_kind: &str,
    source_overrides: Value,
    destination_overrides: Value,
    route_unclassified_to: Option<String>,
) -> Result<ConnectorFlowConfig, AppError> {
    let source_base = match source_kind {
        "imap" => json!({
            "kind": "imap",
            "host": "imap.example.com",
            "port": 993,
            "folder": "INBOX",
            "unreadOnly": true,
            "maxEmails": 1,
            "sinceDate": null,
            "beforeDate": null,
            "passwordEnv": "IMAP_PASSWORD",
            "emailAddress": env::var("IMAP_EMAIL").ok()
        }),
        "gmail" => json!({
            "kind": "imap",
            "preset": "gmail",
            "host": "imap.gmail.com",
            "port": 993,
            "folder": "INBOX",
            "unreadOnly": true,
            "maxEmails": 1,
            "sinceDate": null,
            "beforeDate": null,
            "passwordEnv": "GMAIL_APP_PASSWORD",
            "emailAddress": env::var("GMAIL_EMAIL").ok()
        }),
        _ => {
            return Err(AppError::Message(format!(
                "Unsupported source connector: {source_kind}"
            )))
        }
    };

    let destination_base = match destination_kind {
        "webhook" => json!({
            "kind": "webhook",
            "endpoint": "https://example.com/email-triage",
            "tokenEnv": "WEBHOOK_TOKEN"
        }),
        "joplin" => json!({
            "kind": "joplin",
            "preset": "joplin",
            "endpoint": "http://127.0.0.1:41184",
            "tokenEnv": "JOPLIN_TOKEN",
            "tag": "email-triage"
        }),
        "crm" => json!({
            "kind": "webhook",
            "preset": "crm",
            "endpoint": "https://services.leadconnectorhq.com/email-triage",
            "tokenEnv": "HIGHLEVEL_API_KEY"
        }),
        "highlevel" => json!({
            "kind": "webhook",
            "preset": "highlevel",
            "endpoint": "https://services.leadconnectorhq.com/email-triage",
            "tokenEnv": "HIGHLEVEL_API_KEY"
        }),
        _ => {
            return Err(AppError::Message(format!(
                "Unsupported destination connector: {destination_kind}"
            )))
        }
    };

    let flow_value = json!({
        "source": merge_json(source_base, source_overrides),
        "destination": merge_json(destination_base, destination_overrides),
        "routeUnclassifiedTo": route_unclassified_to
    });

    Ok(serde_json::from_value(flow_value)?)
}

fn run_connector_flow(flow: ConnectorFlowConfig, dry_run: bool) -> Result<FlowRunResult, AppError> {
    let emails = match flow.source.kind.as_str() {
        "imap" | "gmail" => fetch_imap_messages(&flow.source)?,
        _ => {
            return Err(AppError::Message(format!(
                "Unsupported source connector: {}",
                flow.source.kind
            )))
        }
    };

    let mut results = Vec::new();

    for email in emails {
        let mut triage = triage_email(email)?;
        if triage.classification.category == "other" {
            if let Some(route) = &flow.route_unclassified_to {
                triage.classification.suggested_route = Some(route.clone());
            }
        }
        let delivery = if dry_run {
            json!({
                "dryRun": true,
                "destination": flow.destination.kind
            })
        } else {
            deliver_to_destination(&triage, &flow.destination)?
        };

        results.push(json!({
            "triage": triage,
            "delivery": delivery
        }));
    }

    Ok(FlowRunResult {
        source: flow.source.kind,
        destination: flow.destination.kind,
        processed: results.len(),
        results,
    })
}

fn process_queue_jobs(
    flow: &ConnectorFlowConfig,
    limit: Option<usize>,
    dry_run: bool,
) -> Result<Value, AppError> {
    let max_items = limit.unwrap_or(usize::MAX);
    let mut jobs = Vec::new();
    {
        let mut state = app_state()
            .lock()
            .map_err(|_| AppError::Message("App state lock poisoned.".to_string()))?;
        for _ in 0..max_items {
            let Some(job) = state.queue.pop_front() else {
                break;
            };
            jobs.push(job);
        }
    }

    let mut processed = Vec::new();
    for job in jobs {
        set_active_job_status(&job, "classifying", None)?;
        let result = process_single_job(&job, flow, dry_run);
        let item = match result {
            Ok(outcome) => QueueRunItem {
                job_id: job.id,
                email_subject: job.email.subject.clone(),
                success: true,
                outcome,
            },
            Err(error) => QueueRunItem {
                job_id: job.id,
                email_subject: job.email.subject.clone(),
                success: false,
                outcome: json!({ "error": error.to_string() }),
            },
        };
        processed.push(item);
        clear_active_job_status()?;
    }

    {
        let mut state = app_state()
            .lock()
            .map_err(|_| AppError::Message("App state lock poisoned.".to_string()))?;
        state.last_results = processed.clone();
        Ok(json!({
            "processed": processed.len(),
            "remainingQueueDepth": state.queue.len(),
            "results": processed
        }))
    }
}

fn process_single_job(
    job: &QueuedJob,
    flow: &ConnectorFlowConfig,
    dry_run: bool,
) -> Result<Value, AppError> {
    let mut triage = triage_email(job.email.clone())?;
    if triage.classification.category == "other" {
        if let Some(route) = &flow.route_unclassified_to {
            triage.classification.suggested_route = Some(route.clone());
        }
    }

    let delivery = if dry_run {
        json!({
            "dryRun": true,
            "destination": flow.destination.kind
        })
    } else {
        set_active_job_status(job, "delivering", Some(flow.destination.kind.clone()))?;
        deliver_to_destination(&triage, &flow.destination)?
    };

    Ok(json!({
        "jobId": job.id,
        "triage": triage,
        "delivery": delivery
    }))
}

fn set_active_job_status(
    job: &QueuedJob,
    stage: &str,
    detail: Option<String>,
) -> Result<(), AppError> {
    let mut state = app_state()
        .lock()
        .map_err(|_| AppError::Message("App state lock poisoned.".to_string()))?;
    state.active_job = Some(ActiveJobStatus {
        job_id: job.id,
        email_subject: job.email.subject.clone(),
        stage: stage.to_string(),
        detail,
    });
    Ok(())
}

fn clear_active_job_status() -> Result<(), AppError> {
    let mut state = app_state()
        .lock()
        .map_err(|_| AppError::Message("App state lock poisoned.".to_string()))?;
    state.active_job = None;
    Ok(())
}

fn fetch_imap_messages(config: &SourceConnectorConfig) -> Result<Vec<EmailInput>, AppError> {
    let email_address = config
        .email_address
        .clone()
        .or_else(|| env::var("IMAP_EMAIL").ok())
        .or_else(|| env::var("GMAIL_EMAIL").ok())
        .ok_or_else(|| AppError::Message("Missing source email address. Set source.emailAddress, IMAP_EMAIL, or GMAIL_EMAIL.".to_string()))?;
    let password_env = config
        .password_env
        .clone()
        .unwrap_or_else(|| default_imap_password_env(config));
    let password = env::var(&password_env).map_err(|_| {
        AppError::Message(format!(
            "Missing source password env var: {password_env}"
        ))
    })?;
    let host = config
        .host
        .clone()
        .unwrap_or_else(|| default_imap_host(config));
    let port = config.port.unwrap_or(993);
    let folder = config
        .folder
        .clone()
        .unwrap_or_else(|| "INBOX".to_string());
    let max_emails = config.max_emails.unwrap_or(1) as usize;
    let search_query = build_imap_search_query(config)?;
    let timeout = imap_timeout();

    let tls = TlsConnector::builder()
        .build()
        .map_err(|error| AppError::Message(format!("TLS setup failed: {error}")))?;
    let client = connect_imap_with_timeout((host.as_str(), port), host.as_str(), &tls, timeout)?;
    let mut session = client
        .login(email_address, password)
        .map_err(|(error, _)| AppError::Message(format!("IMAP login failed: {error}")))?;

    session
        .select(&folder)
        .map_err(|error| AppError::Message(format!("IMAP select failed: {error}")))?;

    let ids = session
        .search(&search_query)
        .map_err(|error| AppError::Message(format!("IMAP search failed: {error}")))?;

    let mut message_ids: Vec<u32> = ids.into_iter().collect();
    message_ids.sort_unstable();
    message_ids.reverse();
    message_ids.truncate(max_emails);

    let mut emails = Vec::new();
    for id in message_ids {
        let fetches = session
            .fetch(id.to_string(), "RFC822")
            .map_err(|error| AppError::Message(format!("IMAP fetch failed: {error}")))?;
        for fetch in &fetches {
            emails.push(parse_imap_fetch(fetch)?);
        }
    }

    session.logout().ok();
    Ok(emails)
}

fn default_imap_host(config: &SourceConnectorConfig) -> String {
    match config.preset.as_deref().unwrap_or(config.kind.as_str()) {
        "gmail" => "imap.gmail.com".to_string(),
        _ => "imap.example.com".to_string(),
    }
}

fn default_imap_password_env(config: &SourceConnectorConfig) -> String {
    match config.preset.as_deref().unwrap_or(config.kind.as_str()) {
        "gmail" => "GMAIL_APP_PASSWORD".to_string(),
        _ => "IMAP_PASSWORD".to_string(),
    }
}

fn build_imap_search_query(config: &SourceConnectorConfig) -> Result<String, AppError> {
    let mut terms = Vec::new();

    if config.unread_only.unwrap_or(true) {
        terms.push("UNSEEN".to_string());
    } else {
        terms.push("ALL".to_string());
    }

    if let Some(since_date) = &config.since_date {
        terms.push(format!("SINCE {}", imap_date_literal(since_date)?));
    }

    if let Some(before_date) = &config.before_date {
        terms.push(format!("BEFORE {}", imap_date_literal(before_date)?));
    }

    Ok(terms.join(" "))
}

fn imap_date_literal(value: &str) -> Result<String, AppError> {
    let parts: Vec<_> = value.split('-').collect();
    if parts.len() != 3 {
        return Err(AppError::Message(format!(
            "Invalid date '{value}'. Expected YYYY-MM-DD."
        )));
    }

    let year = parts[0];
    let month = match parts[1] {
        "01" => "Jan",
        "02" => "Feb",
        "03" => "Mar",
        "04" => "Apr",
        "05" => "May",
        "06" => "Jun",
        "07" => "Jul",
        "08" => "Aug",
        "09" => "Sep",
        "10" => "Oct",
        "11" => "Nov",
        "12" => "Dec",
        _ => {
            return Err(AppError::Message(format!(
                "Invalid month in date '{value}'. Expected YYYY-MM-DD."
            )))
        }
    };
    let day_num = parts[2]
        .parse::<u32>()
        .map_err(|_| AppError::Message(format!("Invalid day in date '{value}'.")))?;
    if !(1..=31).contains(&day_num) {
        return Err(AppError::Message(format!("Invalid day in date '{value}'.")));
    }

    Ok(format!("{}-{}-{}", day_num, month, year))
}

fn parse_imap_fetch(fetch: &Fetch) -> Result<EmailInput, AppError> {
    let body = fetch
        .body()
        .ok_or_else(|| AppError::Message("IMAP message had no RFC822 body.".to_string()))?;
    let parsed = mailparse::parse_mail(body)
        .map_err(|error| AppError::Message(format!("Failed to parse email: {error}")))?;

    let from = parsed.headers.get_first_value("From").unwrap_or_default();
    let subject = parsed.headers.get_first_value("Subject").unwrap_or_else(|| "(no subject)".to_string());
    let to = first_header_addresses(&parsed, "To");
    let cc = first_header_addresses(&parsed, "Cc");
    let received_at = parsed.headers.get_first_value("Date");
    let (text_body, html_body) = extract_bodies(&parsed);

    let mut headers = HashMap::new();
    for header in &parsed.headers {
        headers.insert(header.get_key().to_string(), header.get_value());
    }

    Ok(EmailInput {
        from,
        to,
        cc,
        subject,
        text_body,
        html_body,
        headers: Some(headers),
        received_at,
    })
}

fn first_header_addresses(parsed: &ParsedMail<'_>, header_name: &str) -> Vec<String> {
    parsed
        .headers
        .iter()
        .find(|header| header.get_key_ref().eq_ignore_ascii_case(header_name))
        .and_then(|header| parse_addresses(header).ok())
        .unwrap_or_default()
}

fn parse_addresses(header: &MailHeader<'_>) -> Result<Vec<String>, AppError> {
    let list = addrparse_header(header)
        .map_err(|error| AppError::Message(format!("Failed to parse address header: {error}")))?;
    let mut out = Vec::new();
    flatten_mail_addrs(&list, &mut out);
    Ok(out)
}

fn flatten_mail_addrs(addrs: &[MailAddr], out: &mut Vec<String>) {
    for addr in addrs {
        match addr {
            MailAddr::Single(info) => out.push(info.addr.clone()),
            MailAddr::Group(group) => {
                for info in &group.addrs {
                    out.push(info.addr.clone());
                }
            }
        }
    }
}

fn extract_bodies(parsed: &ParsedMail<'_>) -> (Option<String>, Option<String>) {
    let mut text_body = None;
    let mut html_body = None;
    collect_bodies(parsed, &mut text_body, &mut html_body);
    (text_body, html_body)
}

fn collect_bodies(
    parsed: &ParsedMail<'_>,
    text_body: &mut Option<String>,
    html_body: &mut Option<String>,
) {
    if parsed.subparts.is_empty() {
        let mimetype = parsed.ctype.mimetype.to_lowercase();
        if mimetype == "text/plain" && text_body.is_none() {
            if let Ok(body) = parsed.get_body() {
                *text_body = Some(body);
            }
        } else if mimetype == "text/html" && html_body.is_none() {
            if let Ok(body) = parsed.get_body() {
                *html_body = Some(body);
            }
        }
        return;
    }

    for part in &parsed.subparts {
        collect_bodies(part, text_body, html_body);
    }
}

fn deliver_to_destination(
    triage: &TriageResult,
    destination: &DestinationConnectorConfig,
) -> Result<Value, AppError> {
    match destination.kind.as_str() {
        "joplin" => create_joplin_note(triage, destination),
        "webhook" | "crm" | "highlevel" => {
            let endpoint = destination
                .endpoint
                .clone()
                .ok_or_else(|| AppError::Message("Webhook destination requires endpoint.".to_string()))?;
            let token_env = destination
                .token_env
                .clone()
                .unwrap_or_else(|| default_webhook_token_env(destination));
            let token = env::var(&token_env).ok();
            let mut headers = HashMap::new();
            if let Some(token) = token {
                headers.insert("authorization".to_string(), format!("Bearer {token}"));
            }
            let route = RoutingConfig {
                endpoint,
                method: Some("POST".to_string()),
                headers: Some(headers),
                include_raw_email: Some(true),
            };
            send_route(triage, &route)
        }
        _ => Err(AppError::Message(format!(
            "Unsupported destination connector: {}",
            destination.kind
        ))),
    }
}

fn default_webhook_token_env(destination: &DestinationConnectorConfig) -> String {
    match destination
        .preset
        .as_deref()
        .unwrap_or(destination.kind.as_str())
    {
        "crm" | "highlevel" => "HIGHLEVEL_API_KEY".to_string(),
        _ => "WEBHOOK_TOKEN".to_string(),
    }
}

fn create_joplin_note(
    triage: &TriageResult,
    destination: &DestinationConnectorConfig,
) -> Result<Value, AppError> {
    let endpoint = destination
        .endpoint
        .clone()
        .unwrap_or_else(|| "http://127.0.0.1:41184".to_string());
    let token_env = destination
        .token_env
        .clone()
        .unwrap_or_else(|| "JOPLIN_TOKEN".to_string());
    let token = env::var(&token_env).map_err(|_| {
        AppError::Message(format!("Missing Joplin token env var: {token_env}"))
    })?;

    let client = http_client();
    let title = format!(
        "[{}] {}",
        triage.classification.category.to_uppercase(),
        triage.email.subject
    );
    let body = build_joplin_body(triage);
    let mut note_payload = serde_json::Map::new();
    note_payload.insert("title".to_string(), Value::String(title));
    note_payload.insert("body".to_string(), Value::String(body));

    if let Some(notebook_id) = &destination.notebook_id {
        note_payload.insert("parent_id".to_string(), Value::String(notebook_id.clone()));
    }

    let url = format!("{}/notes?token={}", endpoint.trim_end_matches('/'), token);
    let note_response = client.post(&url).json(&Value::Object(note_payload)).send()?;
    let status = note_response.status();
    let note_body = note_response.text()?;
    if !status.is_success() {
        return Err(AppError::Message(format!(
            "Joplin note creation failed with status {status}: {note_body}"
        )));
    }
    let created_note: Value = serde_json::from_str(&note_body)?;

    if let Some(tag) = &destination.tag {
        if let Some(note_id) = created_note.get("id").and_then(Value::as_str) {
            ensure_joplin_tag(&client, endpoint.trim_end_matches('/'), &token, note_id, tag)?;
        }
    }

    Ok(json!({
        "delivered": true,
        "status": status.as_u16(),
        "endpoint": endpoint,
        "noteId": created_note.get("id").cloned().unwrap_or(Value::Null),
        "title": created_note.get("title").cloned().unwrap_or(Value::Null)
    }))
}

fn build_joplin_body(triage: &TriageResult) -> String {
    let mut out = String::new();
    out.push_str("# Email Triage\n\n");
    out.push_str(&format!("Subject: {}\n", triage.email.subject));
    out.push_str(&format!("From: {}\n", triage.email.from));
    if !triage.email.to.is_empty() {
        out.push_str(&format!("To: {}\n", triage.email.to.join(", ")));
    }
    if let Some(received_at) = &triage.email.received_at {
        out.push_str(&format!("Date: {}\n", received_at));
    }
    out.push_str(&format!(
        "Category: {} ({:.0}%)\n\n",
        triage.classification.category,
        triage.classification.confidence * 100.0
    ));
    if let Some(priority) = &triage.classification.priority {
        out.push_str(&format!("Priority: {}\n\n", priority));
    }
    out.push_str("## Summary\n\n");
    out.push_str(&triage.classification.summary);
    out.push_str("\n\n## Reasoning\n\n");
    out.push_str(&triage.classification.reasoning);
    if let Some(route) = &triage.classification.suggested_route {
        out.push_str("\n\n## Suggested Route\n\n");
        out.push_str(route);
    }
    if let Some(next_step) = &triage.classification.suggested_next_step {
        out.push_str("\n\n## Suggested Next Step\n\n");
        out.push_str(next_step);
    }
    if !triage.classification.action_items.is_empty() {
        out.push_str("\n\n## Action Items\n\n");
        for item in &triage.classification.action_items {
            out.push_str("- ");
            out.push_str(item);
            out.push('\n');
        }
    }
    if !triage.classification.contact_hints.is_empty() {
        out.push_str("\n\n## Contact Hints\n\n");
        out.push_str(&triage.classification.contact_hints.join(", "));
    }
    if !triage.classification.tags.is_empty() {
        out.push_str("\n\n## Tags\n\n");
        out.push_str(&triage.classification.tags.join(", "));
    }
    if let Some(email_text) = readable_email_text(&triage.email) {
        out.push_str("\n\n## Email Content\n\n");
        out.push_str("```text\n");
        out.push_str(&email_text);
        if !email_text.ends_with('\n') {
            out.push('\n');
        }
        out.push_str("```\n");
    }
    out
}

fn readable_email_text(email: &EmailInput) -> Option<String> {
    if let Some(text_body) = email.text_body.as_deref() {
        let cleaned = normalize_plain_text(text_body);
        if !cleaned.is_empty() {
            return Some(cleaned);
        }
    }

    email
        .html_body
        .as_deref()
        .map(strip_html_to_text)
        .map(|text| normalize_plain_text(&text))
        .filter(|text| !text.is_empty())
}

fn ensure_joplin_tag(
    client: &Client,
    endpoint: &str,
    token: &str,
    note_id: &str,
    tag_title: &str,
) -> Result<(), AppError> {
    let search_url = format!(
        "{}/search?query={}&type=tag&token={}",
        endpoint,
        url_encode(tag_title),
        token
    );
    let existing_response = client.get(&search_url).send()?;
    let existing_status = existing_response.status();
    let existing_body = existing_response.text()?;
    if !existing_status.is_success() {
        return Err(AppError::Message(format!(
            "Joplin tag search failed with status {existing_status}: {existing_body}"
        )));
    }
    let existing: Value = serde_json::from_str(&existing_body)?;
    let tag_id = existing
        .get("items")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("id"))
        .and_then(Value::as_str)
        .map(|id| id.to_string());

    let tag_id = match tag_id {
        Some(id) => id,
        None => {
            let create_url = format!("{}/tags?token={}", endpoint, token);
            let created_response = client
                .post(&create_url)
                .json(&json!({ "title": tag_title }))
                .send()?;
            let created_status = created_response.status();
            let created_body = created_response.text()?;
            if !created_status.is_success() {
                return Err(AppError::Message(format!(
                    "Joplin tag creation failed with status {created_status}: {created_body}"
                )));
            }
            let created: Value = serde_json::from_str(&created_body)?;
            created
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| AppError::Message("Failed to create Joplin tag.".to_string()))?
                .to_string()
        }
    };

    let attach_url = format!("{}/tags/{}/notes/{}?token={}", endpoint, tag_id, note_id, token);
    let attach_response = client
        .post(&attach_url)
        .json(&json!({ "id": note_id }))
        .send()?;
    let attach_status = attach_response.status();
    let attach_body = attach_response.text()?;
    if !attach_status.is_success() {
        return Err(AppError::Message(format!(
            "Joplin tag attach failed with status {attach_status}: {attach_body}"
        )));
    }
    Ok(())
}

fn url_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![b as char]
            }
            _ => format!("%{b:02X}").chars().collect(),
        })
        .collect()
}

fn merge_json(base: Value, overrides: Value) -> Value {
    match (base, overrides) {
        (Value::Object(mut base_map), Value::Object(override_map)) => {
            for (key, override_value) in override_map {
                let base_value = base_map.remove(&key).unwrap_or(Value::Null);
                base_map.insert(key, merge_json(base_value, override_value));
            }
            Value::Object(base_map)
        }
        (_, override_value) if !override_value.is_null() => override_value,
        (base_value, _) => base_value,
    }
}

fn build_classifier_prompt(email: &EmailInput) -> String {
    let payload = classifier_email_view(email);
    format!(
        "You are an email triage classifier.\n\
Classify the email into exactly one of: sales, support, billing, spam, personal, urgent, other.\n\
Infer CRM-friendly metadata when appropriate.\n\
Priority must be one of: low, medium, high, urgent.\n\
Return strict JSON only with this shape:\n\
{{\"category\":\"support\",\"confidence\":0.94,\"summary\":\"...\",\"reasoning\":\"...\",\"suggestedRoute\":\"...\",\"priority\":\"high\",\"suggestedNextStep\":\"...\",\"actionItems\":[\"...\"],\"contactHints\":[\"...\"],\"tags\":[\"...\"]}}\n\n\
Email payload:\n{}",
        serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".to_string())
    )
}

fn classifier_email_view(email: &EmailInput) -> Value {
    let text_body = readable_email_text(email).map(|body| truncate_chars(&body, 600));

    json!({
        "from": email.from,
        "to": email.to,
        "cc": email.cc,
        "subject": email.subject,
        "receivedAt": email.received_at,
        "textBody": text_body
    })
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (count, ch) in value.chars().enumerate() {
        if count >= max_chars {
            out.push_str("...[truncated]");
            break;
        }
        out.push(ch);
    }
    out
}

fn strip_html_to_text(html: &str) -> String {
    let mut out = String::new();
    let mut chars = html.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '<' => {
                let mut tag = String::new();
                while let Some(next) = chars.next() {
                    if next == '>' {
                        break;
                    }
                    tag.push(next);
                }
                if is_block_html_tag(&tag) && !out.ends_with('\n') {
                    out.push('\n');
                }
            }
            '&' => {
                let mut entity = String::new();
                while let Some(&next) = chars.peek() {
                    entity.push(next);
                    chars.next();
                    if next == ';' || entity.len() > 10 {
                        break;
                    }
                }
                out.push_str(&decode_html_entity(&entity));
            }
            _ => out.push(ch),
        }
    }

    out
}

fn is_block_html_tag(tag: &str) -> bool {
    let trimmed = tag.trim().trim_start_matches('/').to_ascii_lowercase();
    matches!(
        trimmed.split_whitespace().next().unwrap_or_default(),
        "p" | "div" | "br" | "li" | "ul" | "ol" | "table" | "tr" | "td" | "section" | "article" | "header" | "footer" | "h1" | "h2" | "h3" | "h4" | "h5" | "h6"
    )
}

fn decode_html_entity(entity: &str) -> String {
    match entity {
        "nbsp;" => " ".to_string(),
        "amp;" => "&".to_string(),
        "lt;" => "<".to_string(),
        "gt;" => ">".to_string(),
        "quot;" => "\"".to_string(),
        "#39;" => "'".to_string(),
        _ => String::new(),
    }
}

fn normalize_plain_text(value: &str) -> String {
    let mut out = String::new();
    let mut blank_run = 0;

    for line in value.lines() {
        let trimmed = line.split_whitespace().collect::<Vec<_>>().join(" ");
        if trimmed.is_empty() {
            blank_run += 1;
            if blank_run <= 1 && !out.is_empty() {
                out.push('\n');
            }
            continue;
        }

        blank_run = 0;
        if !out.is_empty() && !out.ends_with('\n') {
            out.push('\n');
        }
        out.push_str(&trimmed);
    }

    out.trim().to_string()
}

fn parse_classifier_output(stdout: &str) -> Result<ClassificationResult, AppError> {
    let trimmed = stdout.trim();
    let parsed = if let Some(parsed) = parse_json_candidate(trimmed) {
        parsed
    } else {
        let mut parsed = None;
        for (index, _) in trimmed.match_indices('{').rev() {
            if let Some(candidate) = parse_json_candidate(&trimmed[index..]) {
                parsed = Some(candidate);
                break;
            }
        }

        parsed.ok_or_else(|| AppError::Message("Classifier did not return valid JSON.".to_string()))?
    };

    match parsed.category.as_str() {
        "sales" | "support" | "billing" | "spam" | "personal" | "urgent" | "other" => {}
        _ => {
            return Err(AppError::Message(format!(
                "Unsupported category returned by classifier: {}",
                parsed.category
            )))
        }
    }

    if !(0.0..=1.0).contains(&parsed.confidence) {
        return Err(AppError::Message(
            "Classifier confidence must be between 0 and 1.".to_string(),
        ));
    }

    if let Some(priority) = parsed.priority.as_deref() {
        match priority {
            "low" | "medium" | "high" | "urgent" => {}
            _ => {
                return Err(AppError::Message(format!(
                    "Unsupported priority returned by classifier: {priority}"
                )))
            }
        }
    }

    Ok(parsed)
}

fn parse_json_candidate(candidate: &str) -> Option<ClassificationResult> {
    let candidate = candidate.trim();
    if !candidate.starts_with('{') {
        return None;
    }

    let mut deserializer = Deserializer::from_str(candidate);
    let parsed = ClassificationResult::deserialize(&mut deserializer).ok()?;
    if deserializer.end().is_ok() {
        Some(parsed)
    } else {
        None
    }
}

fn run_classifier(classifier: &str, prompt: &str) -> Result<std::process::Output, AppError> {
    let shell = env::var("CLASSIFIER_SHELL").ok();

    if let Some(shell) = shell {
        return Command::new(shell)
            .arg(shell_flag())
            .arg(classifier)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .and_then(|child| write_prompt_and_wait(child, prompt))
            .map_err(AppError::from);
    }

    let mut parts = shlex::split(classifier)
        .ok_or_else(|| AppError::Message("CLASSIFIER_COMMAND has invalid shell quoting.".to_string()))?
        .into_iter();
    let program = parts
        .next()
        .ok_or_else(|| AppError::Message("CLASSIFIER_COMMAND is empty.".to_string()))?;
    let args: Vec<_> = parts.collect();

    Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .and_then(|child| write_prompt_and_wait(child, prompt))
        .map_err(AppError::from)
}

fn http_client() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        Client::builder()
            .connect_timeout(http_connect_timeout())
            .timeout(http_request_timeout())
            .build()
            .expect("HTTP client should build")
    })
}

fn shell_flag() -> &'static str {
    if cfg!(windows) {
        "/C"
    } else {
        "-lc"
    }
}

fn write_prompt_and_wait(mut child: Child, prompt: &str) -> Result<std::process::Output, io::Error> {
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(prompt.as_bytes())?;
        stdin.flush()?;
    }
    let timeout = classifier_timeout();
    let start = Instant::now();

    loop {
        if child.try_wait()?.is_some() {
            return child.wait_with_output();
        }

        if start.elapsed() >= timeout {
            child.kill()?;
            let status = child.wait()?;
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "Classifier timed out after {}s and was terminated (status: {}).",
                    timeout.as_secs(),
                    exit_status_label(status)
                ),
            ));
        }

        thread::sleep(Duration::from_millis(100));
    }
}

fn classifier_timeout() -> Duration {
    env_duration_secs("CLASSIFIER_TIMEOUT_SECS", 45)
}

fn http_connect_timeout() -> Duration {
    env_duration_secs("HTTP_CONNECT_TIMEOUT_SECS", 5)
}

fn http_request_timeout() -> Duration {
    env_duration_secs("HTTP_REQUEST_TIMEOUT_SECS", 15)
}

fn imap_timeout() -> Duration {
    env_duration_secs("IMAP_TIMEOUT_SECS", 15)
}

fn env_duration_secs(name: &str, default_secs: u64) -> Duration {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(default_secs))
}

fn exit_status_label(status: ExitStatus) -> String {
    status
        .code()
        .map(|code| code.to_string())
        .unwrap_or_else(|| "signal".to_string())
}

fn connect_imap_with_timeout<A: ToSocketAddrs, S: AsRef<str>>(
    addr: A,
    domain: S,
    tls_connector: &TlsConnector,
    timeout: Duration,
) -> Result<imap::Client<native_tls::TlsStream<TcpStream>>, AppError> {
    let mut last_error = None;

    for socket_addr in addr.to_socket_addrs()? {
        match connect_single_imap(socket_addr, domain.as_ref(), tls_connector, timeout) {
            Ok(client) => return Ok(client),
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error.unwrap_or_else(|| {
        AppError::Message("IMAP connect failed: no socket addresses resolved.".to_string())
    }))
}

fn connect_single_imap(
    addr: SocketAddr,
    domain: &str,
    tls_connector: &TlsConnector,
    timeout: Duration,
) -> Result<imap::Client<native_tls::TlsStream<TcpStream>>, AppError> {
    let tcp_stream = TcpStream::connect_timeout(&addr, timeout)
        .map_err(|error| AppError::Message(format!("IMAP TCP connect failed for {addr}: {error}")))?;
    tcp_stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| AppError::Message(format!("IMAP read timeout setup failed: {error}")))?;
    tcp_stream
        .set_write_timeout(Some(timeout))
        .map_err(|error| AppError::Message(format!("IMAP write timeout setup failed: {error}")))?;

    let tls_stream = tls_connector
        .connect(domain, tcp_stream)
        .map_err(|error| AppError::Message(format!("IMAP TLS handshake failed for {addr}: {error}")))?;

    let mut client = imap::Client::new(tls_stream);
    client
        .read_greeting()
        .map_err(|error| AppError::Message(format!("IMAP greeting failed for {addr}: {error}")))?;
    Ok(client)
}

fn send_route(triage: &TriageResult, routing: &RoutingConfig) -> Result<Value, AppError> {
    let client = http_client();
    let method = routing.method.clone().unwrap_or_else(|| "POST".to_string());
    let include_raw_email = routing.include_raw_email.unwrap_or(true);
    let mut request = client.request(method.parse().map_err(|_| {
        AppError::Message(format!("Unsupported HTTP method for routing: {method}"))
    })?, &routing.endpoint);

    if let Some(headers) = &routing.headers {
        for (key, value) in headers {
            request = request.header(key, value);
        }
    }

    let body = if include_raw_email {
        json!({
            "classification": triage.classification,
            "email": triage.email,
            "metadata": {
                "classifier": triage.classifier
            }
        })
    } else {
        json!({
            "classification": triage.classification,
            "metadata": {
                "classifier": triage.classifier
            }
        })
    };

    let response = request.json(&body).send()?;
    let status = response.status().as_u16();
    let text = response.text()?;
    let snippet = if text.len() > 400 {
        format!("{}...", &text[..397])
    } else {
        text
    };

    Ok(json!({
        "delivered": (200..300).contains(&status),
        "status": status,
        "endpoint": routing.endpoint,
        "responseSnippet": snippet
    }))
}

fn tool_result(value: Value) -> Value {
    json!({
        "content": [
            {
                "type": "text",
                "text": serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string())
            }
        ],
        "structuredContent": value
    })
}

fn tool_error_result(message: String) -> Value {
    json!({
        "content": [
            {
                "type": "text",
                "text": message
            }
        ],
        "isError": true
    })
}

fn read_message(reader: &mut BufReader<impl Read>) -> Result<Option<Vec<u8>>, AppError> {
    let mut content_length: Option<usize> = None;

    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            return Ok(None);
        }

        if line.trim().is_empty() {
            break;
        }

        let normalized = line.trim();
        if let Some((name, value)) = normalized.split_once(':') {
            if !name.eq_ignore_ascii_case("Content-Length") {
                continue;
            }
            let parsed = value
                .trim()
                .parse::<usize>()
                .map_err(|_| AppError::Message("Invalid Content-Length header.".to_string()))?;
            content_length = Some(parsed);
        }
    }

    let length = content_length
        .ok_or_else(|| AppError::Message("Missing Content-Length header.".to_string()))?;
    let mut buffer = vec![0; length];
    reader.read_exact(&mut buffer)?;
    Ok(Some(buffer))
}

fn write_message(writer: &mut impl Write, response: &JsonRpcResponse) -> Result<(), AppError> {
    let body = serde_json::to_vec(response)?;
    write!(writer, "Content-Length: {}\r\n\r\n", body.len())?;
    writer.write_all(&body)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn falls_back_for_unsupported_protocol_versions() {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(1)),
            method: "initialize".to_string(),
            params: Some(json!({ "protocolVersion": "2099-01-01" })),
        };
        let result = handle_request(request).unwrap();
        assert_eq!(result.get("protocolVersion"), Some(&json!("2025-11-25")));
    }

    #[test]
    fn parses_rfc_comma_addresses() {
        let (header, _) =
            mailparse::parse_header(b"To: \"Doe, Jane\" <jane@example.com>, team@example.com\r\n")
                .unwrap();
        let parsed = parse_addresses(&header).unwrap();
        assert_eq!(parsed, vec!["jane@example.com", "team@example.com"]);
    }

    #[test]
    fn parse_json_candidate_rejects_trailing_text() {
        assert!(parse_json_candidate("{\"category\":\"support\",\"confidence\":0.9,\"summary\":\"a\",\"reasoning\":\"b\",\"tags\":[]} trailing").is_none());
    }

    #[test]
    fn parse_classifier_output_backfills_new_crm_fields() {
        let parsed = parse_classifier_output(
            "{\"category\":\"support\",\"confidence\":0.9,\"summary\":\"a\",\"reasoning\":\"b\",\"tags\":[\"auth\"]}",
        )
        .unwrap();
        assert!(parsed.priority.is_none());
        assert!(parsed.suggested_next_step.is_none());
        assert!(parsed.action_items.is_empty());
        assert!(parsed.contact_hints.is_empty());
    }

    #[test]
    fn parse_classifier_output_rejects_invalid_priority() {
        let error = parse_classifier_output(
            "{\"category\":\"support\",\"confidence\":0.9,\"summary\":\"a\",\"reasoning\":\"b\",\"priority\":\"critical\",\"tags\":[]}",
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("Unsupported priority returned by classifier"));
    }

    #[test]
    fn heuristic_marks_bulk_marketing_as_spam() {
        let mut headers = HashMap::new();
        headers.insert("List-Unsubscribe".to_string(), "<mailto:unsubscribe@example.com>".to_string());
        let email = EmailInput {
            from: "marketing@example.com".to_string(),
            to: vec!["user@example.com".to_string()],
            cc: vec![],
            subject: "Limited time offer just for you".to_string(),
            text_body: Some("Save big with this coupon and unsubscribe any time.".to_string()),
            html_body: None,
            headers: Some(headers),
            received_at: None,
        };

        let result = heuristic_classification(&email).expect("expected heuristic classification");
        assert_eq!(result.category, "spam");
        assert_eq!(result.suggested_route.as_deref(), Some("bulk-mail"));
    }

    #[test]
    fn gmail_search_query_includes_date_bounds() {
        let config = SourceConnectorConfig {
            kind: "imap".to_string(),
            preset: Some("gmail".to_string()),
            email_address: None,
            password_env: None,
            host: None,
            port: None,
            folder: None,
            unread_only: Some(true),
            max_emails: Some(1),
            since_date: Some("2026-03-20".to_string()),
            before_date: Some("2026-03-21".to_string()),
        };

        let query = build_imap_search_query(&config).unwrap();
        assert_eq!(query, "UNSEEN SINCE 20-Mar-2026 BEFORE 21-Mar-2026");
    }

    #[test]
    fn build_connector_flow_maps_presets_to_generic_transports() {
        let flow = build_connector_flow("gmail", "highlevel", json!({}), json!({}), None).unwrap();
        assert_eq!(flow.source.kind, "imap");
        assert_eq!(flow.source.preset.as_deref(), Some("gmail"));
        assert_eq!(flow.destination.kind, "webhook");
        assert_eq!(flow.destination.preset.as_deref(), Some("highlevel"));
        assert_eq!(flow.destination.token_env.as_deref(), Some("HIGHLEVEL_API_KEY"));
    }

    #[test]
    fn html_is_stripped_to_readable_text() {
        let text = strip_html_to_text("<p>Hello&nbsp;<strong>world</strong></p><p>Reset&nbsp;password</p>");
        let normalized = normalize_plain_text(&text);
        assert_eq!(normalized, "Hello world\nReset password");
    }
}
