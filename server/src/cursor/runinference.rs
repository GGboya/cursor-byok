//! Serves Cursor's RunInference agent-harness protocol from BYOK providers.
//!
//! Cursor 3.27+ downloads an agent-harness module that runs the agent loop
//! client-side and asks the backend to run model invocations only. The module
//! speaks `agent.v1.AgentService` RunSSE/BidiAppend transports like the legacy
//! client, but the Bidi payloads are `RunInferenceClientMessage` envelopes
//! instead of `AgentClientMessage`. This module decodes those envelopes and
//! bridges each `invoke_model` to the configured provider, streaming deltas
//! back as `RunInferenceServerMessage` frames on the same transport.
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use futures_util::StreamExt;
use prost::Message;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{
    cursor::{
        protocol::{
            connect,
            proto::{aiserver::v1 as ai, runinference::v1 as ri},
        },
        transport::TransportRegistry,
    },
    model::{
        ContentPart, ModelInvocation, ModelRequest, ModelSpec, PromptSpec, ProjectedContent,
        ProjectedMessage, ReasoningSpec, Role, ToolCallContent, ToolDefinition,
        ToolResultContent,
    },
    provider::{ModelEvent, Provider},
    Error, Result,
};

struct RunState {
    conversation_id: String,
    model: ModelSpec,
    invocation: u64,
    cancellations: HashMap<String, CancellationToken>,
}

fn states() -> &'static Mutex<HashMap<String, RunState>> {
    static STATES: OnceLock<Mutex<HashMap<String, RunState>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn decode(payload: &[u8]) -> Result<ri::RunInferenceClientMessage> {
    ri::RunInferenceClientMessage::decode(payload)
        .map_err(|error| Error::Protocol(format!("invalid RunInferenceClientMessage: {error}")))
}

/// The model a RunInference message selects, for BYOK-vs-upstream routing.
pub fn requested_model_id(message: &ri::RunInferenceClientMessage) -> Option<String> {
    match message.message.as_ref()? {
        ri::run_inference_client_message::Message::RunRequest(run) => run
            .requested_model
            .as_ref()
            .map(|model| model.model_id.clone())
            .filter(|id| !id.is_empty()),
        ri::run_inference_client_message::Message::InvokeModel(invoke) => invoke
            .request
            .as_ref()
            .and_then(|request| {
                request
                    .requested_model
                    .as_ref()
                    .map(|model| model.model_id.clone())
                    .or_else(|| request.model_id.clone())
            })
            .filter(|id| !id.is_empty()),
        _ => None,
    }
}

pub async fn handle_append(
    registry: &TransportRegistry,
    request_id: &str,
    seqno: i64,
    message: ri::RunInferenceClientMessage,
) -> Result<ai::BidiAppendResponse> {
    let Some(kind) = message.message else {
        return Ok(ai::BidiAppendResponse {});
    };
    match kind {
        ri::run_inference_client_message::Message::RunRequest(run) => {
            let replace_closing = true;
            let handle = registry
                .get_or_create_for_append(request_id, replace_closing)
                .await?;
            let _admission = handle.admit()?;
            let model = model_spec(run.requested_model.as_ref(), None);
            if !run.conversation_id.is_empty() {
                handle.set_conversation_id(&run.conversation_id)?;
                states()
                    .lock()
                    .await
                    .insert(
                        request_id.to_owned(),
                        RunState {
                            conversation_id: run.conversation_id.clone(),
                            model,
                            invocation: 0,
                            cancellations: HashMap::new(),
                        },
                    );
            }
            let resolved = run.requested_model.clone().unwrap_or_default();
            let ready = ri::RunInferenceServerMessage {
                message: Some(ri::run_inference_server_message::Message::RunReady(
                    ri::RunInferenceRunReady {
                        resolved_model: Some(resolved),
                        accepts_client_heartbeat: true,
                        ..Default::default()
                    },
                )),
            };
            emit(&handle, &ready)?;
            let run_model_id = states()
                .lock()
                .await
                .get(request_id)
                .map(|state| state.model.model_id.clone())
                .unwrap_or_default();
            tracing::info!(request_id, seqno, model_id = %run_model_id, "RunInference run ready");
        }
        ri::run_inference_client_message::Message::InvokeModel(invoke) => {
            let provider = registry.provider();
            let handle = registry
                .get_or_create_for_append(request_id, false)
                .await?;
            let _admission = handle.admit()?;
            let state_model = states()
                .lock()
                .await
                .get(request_id)
                .map(|state| state.model.clone());
            let request = invoke.request.unwrap_or_default();
            let model = model_spec(
                request.requested_model.as_ref(),
                request.model_config.as_ref(),
            )
            .with_fallback(state_model);
            let invocation_id = invoke.invocation_id.clone();
            let conversation_id = states()
                .lock()
                .await
                .get(request_id)
                .map(|state| state.conversation_id.clone())
                .unwrap_or_default();
            let index = {
                let mut states = states().lock().await;
                let Some(state) = states.get_mut(request_id) else {
                    return Err(Error::Protocol(
                        "RunInference invoke_model before run_request".into(),
                    ));
                };
                state.invocation += 1;
                state.invocation
            };
            let invocation = build_invocation(
                request_id,
                &conversation_id,
                index,
                model,
                &request,
            )?;
            let cancellation = CancellationToken::new();
            if let Some(state) = states().lock().await.get_mut(request_id) {
                state
                    .cancellations
                    .insert(invocation_id.clone(), cancellation.clone());
            }
            let handle = Arc::new(handle);
            let invocation_id_for_task = invocation_id.clone();
            let provider = provider;
            tokio::spawn(async move {
                let stream = provider.stream(invocation, cancellation);
                tokio::pin!(stream);
                while let Some(event) = stream.next().await {
                    let event = match event {
                        Ok(event) => event,
                        Err(error) => {
                            emit(
                                handle.as_ref(),
                                &invocation_error(&invocation_id_for_task, &error.to_string()),
                            )
                            .ok();
                            break;
                        }
                    };
                    if !emit_event(handle.as_ref(), &invocation_id_for_task, &event) {
                        break;
                    }
                }
                let _ = emit(
                    handle.as_ref(),
                    &ri::RunInferenceServerMessage {
                        message: Some(
                            ri::run_inference_server_message::Message::InvocationEnd(
                                ri::RunInferenceInvocationEnd {
                                    invocation_id: invocation_id_for_task.clone(),
                                    ..Default::default()
                                },
                            ),
                        ),
                    },
                );
            });
        }
        ri::run_inference_client_message::Message::CancelInvocation(cancel) => {
            let mut states = states().lock().await;
            if let Some(state) = states.get_mut(request_id) {
                if let Some(token) = state.cancellations.remove(&cancel.invocation_id) {
                    token.cancel();
                }
            }
        }
        ri::run_inference_client_message::Message::FinishRun(_) => {
            if let Ok(handle) = registry.get_or_create_for_append(request_id, false).await {
                let mut states = states().lock().await;
                if let Some(state) = states.get_mut(request_id) {
                    for (_, token) in state.cancellations.drain() {
                        token.cancel();
                    }
                }
                let _ = handle.emit_frame(connect::encode_end_stream());
            }
            states().lock().await.remove(request_id);
        }
        ri::run_inference_client_message::Message::ClientHeartbeat(_) => {
            if let Ok(handle) = registry.get_or_create_for_append(request_id, false).await {
                emit(
                    &handle,
                    &ri::RunInferenceServerMessage {
                        message: Some(ri::run_inference_server_message::Message::Heartbeat(
                            ri::RunInferenceHeartbeat::default(),
                        )),
                    },
                )?;
            }
        }
        ri::run_inference_client_message::Message::AgentEvent(_)
        | ri::run_inference_client_message::Message::RunRequestLog(_)
        | ri::run_inference_client_message::Message::ResolveSubagentModels(_)
        | ri::run_inference_client_message::Message::ClientFirstCallTimings(_)
        | ri::run_inference_client_message::Message::DeltaBaseSeed(_) => {}
    }
    Ok(ai::BidiAppendResponse {})
}

fn emit(
    handle: &crate::cursor::transport::TransportHandle,
    message: &ri::RunInferenceServerMessage,
) -> Result<()> {
    handle.emit_frame(connect::encode_message(message)?);
    Ok(())
}

fn invocation_error(invocation_id: &str, message: &str) -> ri::RunInferenceServerMessage {
    ri::RunInferenceServerMessage {
        message: Some(ri::run_inference_server_message::Message::InvocationResponse(
            ri::RunInferenceInvocationResponse {
                invocation_id: invocation_id.to_owned(),
                response: Some(ri::InferenceStreamResponse {
                    response: Some(ri::inference_stream_response::Response::Error(
                        ri::InferenceStreamError {
                            message: message.to_owned(),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                }),
            },
        )),
    }
}

/// Maps a normalized provider event into a RunInference delta frame.
/// Returns false when the invocation is finished and no further events apply.
fn emit_event(
    handle: &crate::cursor::transport::TransportHandle,
    invocation_id: &str,
    event: &ModelEvent,
) -> bool {
    let response = match event {
        ModelEvent::Start { .. } => return true,
        ModelEvent::TextStart | ModelEvent::TextEnd | ModelEvent::ThinkingStart
        | ModelEvent::ThinkingEnd => return true,
        ModelEvent::TextDelta(text) => ri::InferenceStreamResponse {
            response: Some(ri::inference_stream_response::Response::TextPart(
                ri::InferenceTextStreamPart {
                    text: text.clone(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        },
        ModelEvent::ThinkingDelta(text) => ri::InferenceStreamResponse {
            response: Some(ri::inference_stream_response::Response::ThinkingPart(
                ri::InferenceThinkingStreamPart {
                    text: text.clone(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        },
        ModelEvent::ToolCallStart {
            index,
            call_id,
            name,
        } => ri::InferenceStreamResponse {
            response: Some(ri::inference_stream_response::Response::ToolCallPart(
                ri::InferenceToolCallStreamPart {
                    tool_call_id: call_id.clone(),
                    tool_name: name.clone(),
                    args: String::new(),
                    is_complete: false,
                    tool_index: Some(*index as i32),
                    call_signature: None,
                },
            )),
            ..Default::default()
        },
        ModelEvent::ToolCallArgumentsDelta { index, delta } => ri::InferenceStreamResponse {
            response: Some(ri::inference_stream_response::Response::ToolCallPart(
                ri::InferenceToolCallStreamPart {
                    tool_call_id: String::new(),
                    tool_name: String::new(),
                    args: delta.clone(),
                    is_complete: false,
                    tool_index: Some(*index as i32),
                    call_signature: None,
                },
            )),
            ..Default::default()
        },
        ModelEvent::ToolCallEnd { index } => ri::InferenceStreamResponse {
            response: Some(ri::inference_stream_response::Response::ToolCallPart(
                ri::InferenceToolCallStreamPart {
                    tool_call_id: String::new(),
                    tool_name: String::new(),
                    args: String::new(),
                    is_complete: true,
                    tool_index: Some(*index as i32),
                    call_signature: None,
                },
            )),
            ..Default::default()
        },
        ModelEvent::ProviderReplayState(_) => return true,
        ModelEvent::Usage(usage) => ri::InferenceStreamResponse {
            response: Some(ri::inference_stream_response::Response::ExtendedUsage(
                ri::InferenceExtendedUsageInfo {
                    input_tokens: usage.input_tokens.unwrap_or(0).min(i32::MAX as u64) as i32,
                    output_tokens: usage.output_tokens.unwrap_or(0).min(i32::MAX as u64) as i32,
                    cache_read_tokens: usage
                        .cache_read_tokens
                        .unwrap_or(0)
                        .min(i32::MAX as u64)
                        as i32,
                    cache_write_tokens: usage
                        .cache_write_tokens
                        .unwrap_or(0)
                        .min(i32::MAX as u64)
                        as i32,
                    max_tokens: 0,
                    reasoning_tokens: usage.reasoning_tokens.map(|value| {
                        value.min(i32::MAX as u64) as i32
                    }),
                },
            )),
            ..Default::default()
        },
        ModelEvent::Done(_) => {
            // Invocation completion is signaled by the caller with InvocationEnd.
            return false;
        }
    };
    let frame = ri::RunInferenceServerMessage {
        message: Some(ri::run_inference_server_message::Message::InvocationResponse(
            ri::RunInferenceInvocationResponse {
                invocation_id: invocation_id.to_owned(),
                response: Some(response),
            },
        )),
    };
    emit(handle, &frame).is_ok()
}

fn model_spec(
    model: Option<&ri::InferenceRequestedModel>,
    config: Option<&ri::InferenceModelConfig>,
) -> ModelSpec {
    let mut spec = ModelSpec {
        model_id: model.map(|model| model.model_id.clone()).unwrap_or_default(),
        display_name: None,
        reasoning: ReasoningSpec {
            enabled: false,
            effort: None,
        },
        latency: crate::model::ModelLatency::Standard,
        max_output_tokens: config.and_then(|config| {
            config
                .max_tokens
                .filter(|tokens| *tokens > 0)
                .map(|tokens| tokens as u64)
        }),
        context_window_tokens: None,
        supports_image_generation: false,
        extra_params: serde_json::json!({}),
    };
    if let Some(model) = model {
        spec.reasoning.enabled = model.max_mode;
        for parameter in &model.parameters {
            match parameter.id.as_str() {
                "effort" | "reasoning" => {
                    let effort = parameter.value.trim();
                    spec.reasoning.effort =
                        (effort != "none" && !effort.is_empty()).then(|| effort.to_string());
                    spec.reasoning.enabled |= spec.reasoning.effort.is_some();
                }
                "thinking" => spec.reasoning.enabled |= parameter.value == "true",
                "fast" => {
                    if parameter.value == "true" {
                        spec.latency = crate::model::ModelLatency::Fast;
                    }
                }
                _ => {}
            }
        }
    }
    spec
}

impl ModelSpec {
    fn with_fallback(self, fallback: Option<ModelSpec>) -> Self {
        match fallback {
            Some(fallback) if self.model_id.is_empty() => fallback,
            _ => self,
        }
    }
}

fn build_invocation(
    request_id: &str,
    conversation_id: &str,
    index: u64,
    model: ModelSpec,
    request: &ri::InferenceStreamRequest,
) -> Result<ModelInvocation> {
    let mut instructions = String::new();
    let mut history = Vec::new();
    for (position, message) in request.messages.iter().enumerate() {
        let role = ri_role(message.role);
        match role {
            Role::System => {
                if !instructions.is_empty() {
                    instructions.push_str("\n\n");
                }
                instructions.push_str(&core_message_text(message));
                continue;
            }
            Role::User => history.push(ProjectedMessage {
                message_id: format!("ri-{request_id}-{position}"),
                role: Role::User,
                content: ProjectedContent::Parts(content_parts(message)),
            }),
            Role::Assistant => {
                let mut calls = Vec::new();
                for (call_index, call) in message.tool_calls.iter().enumerate() {
                    calls.push(ToolCallContent {
                        index: call_index,
                        call_id: call.tool_call_id.clone(),
                        name: call.tool_name.clone(),
                        arguments: struct_value(&call.args),
                    });
                }
                history.push(ProjectedMessage {
                    message_id: format!("ri-{request_id}-{position}"),
                    role: Role::Assistant,
                    content: ProjectedContent::Assistant {
                        text: core_message_text(message),
                        thinking: reasoning_text(message),
                        replay_state: None,
                        calls,
                    },
                });
            }
            Role::Tool => {
                let Some(ri::inference_core_message::Content::ToolContent(tool_content)) =
                    message.content.as_ref()
                else {
                    return Err(Error::Protocol("tool message without content".into()));
                };
                let result = tool_result(tool_content);
                history.push(ProjectedMessage {
                    message_id: format!("ri-{request_id}-{position}"),
                    role: Role::Tool,
                    content: ProjectedContent::ToolResult(result),
                });
            }
        }
    }
    let tools = request
        .tools
        .iter()
        .map(|tool| ToolDefinition {
            name: tool.name.clone(),
            description: tool.description.clone(),
            parameters: struct_value(&tool.parameters),
        })
        .collect();
    Ok(ModelInvocation {
        call_id: format!("ri-{request_id}-{index}"),
        run_id: request_id.to_owned(),
        conversation_id: conversation_id.to_owned(),
        provider_call_index: index.saturating_sub(1),
        request: ModelRequest {
            prompt: PromptSpec {
                instructions,
                tools,
            },
            model,
            history,
        },
    })
}

fn ri_role(role: i32) -> Role {
    if role == ri::InferenceMessageRole::User as i32 {
        Role::User
    } else if role == ri::InferenceMessageRole::Assistant as i32 {
        Role::Assistant
    } else if role == ri::InferenceMessageRole::Tool as i32 {
        Role::Tool
    } else if role == ri::InferenceMessageRole::System as i32 {
        Role::System
    } else {
        Role::User
    }
}

fn core_message_text(message: &ri::InferenceCoreMessage) -> String {
    match message.content.as_ref() {
        Some(ri::inference_core_message::Content::Text(text)) => text.clone(),
        Some(ri::inference_core_message::Content::Parts(parts)) => parts
            .parts
            .iter()
            .filter_map(|part| match part.part.as_ref() {
                Some(ri::inference_content_part::Part::Text(text)) => {
                    Some(text.text.clone())
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(""),
        _ => String::new(),
    }
}

fn content_parts(message: &ri::InferenceCoreMessage) -> Vec<ContentPart> {
    let mut parts = Vec::new();
    match message.content.as_ref() {
        Some(ri::inference_core_message::Content::Text(text)) => {
            parts.push(ContentPart::Text { text: text.clone() });
        }
        Some(ri::inference_core_message::Content::Parts(content)) => {
            for part in &content.parts {
                match part.part.as_ref() {
                    Some(ri::inference_content_part::Part::Text(text)) => {
                        parts.push(ContentPart::Text {
                            text: text.text.clone(),
                        });
                    }
                    Some(ri::inference_content_part::Part::Image(image)) => {
                        if let Ok(data) = base64::Engine::decode(
                            &base64::engine::general_purpose::STANDARD,
                            image.data.as_bytes(),
                        ) {
                            parts.push(ContentPart::Image {
                                mime_type: image
                                    .mime_type
                                    .clone()
                                    .unwrap_or_else(|| "image/png".into()),
                                data,
                            });
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    if parts.is_empty() {
        if let Some(text) = message.assistant_text_parts.as_deref().filter(|t| !t.is_empty()) {
            parts.push(ContentPart::Text { text: text.to_owned() });
        }
    }
    parts
}

fn reasoning_text(message: &ri::InferenceCoreMessage) -> String {
    message
        .reasoning_parts
        .iter()
        .map(|part| part.text.as_str())
        .collect::<Vec<_>>()
        .join("")
}

fn tool_result(content: &ri::InferenceToolResultContent) -> ToolResultContent {
    let mut result = ToolResultContent {
        call_id: String::new(),
        name: String::new(),
        content: String::new(),
        is_error: false,
        image: None,
        provider_parts: Vec::new(),
    };
    if let Some(parts) = content.parts.as_ref() {
        result.call_id = parts.tool_call_id.clone();
        result.name = parts.tool_name.clone();
        result.is_error = parts.is_error;
        if let Some(value) = parts.result.as_ref() {
            result.content = prost_value_to_string(value);
        }
    }
    result
}

fn prost_value_to_string(value: &prost_types::Value) -> String {
    match &value.kind {
        Some(prost_types::value::Kind::StringValue(text)) => text.clone(),
        _ => {
            let json = prost_types_to_json(value);
            serde_json::to_string(&json).unwrap_or_default()
        }
    }
}

/// Converts a protobuf Struct into a JSON value (tool schemas and arguments).
fn struct_value(value: &Option<prost_types::Struct>) -> serde_json::Value {
    // The generated google.protobuf.Struct maps fields as
    // map<string, prost_types::Value>; prost_types::Value implements Into.
    match value {
        Some(struct_value) => prost_types_to_json_fields(struct_value),
        None => serde_json::json!({}),
    }
}

fn prost_types_to_json_fields(struct_value: &prost_types::Struct) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (key, value) in &struct_value.fields {
        map.insert(key.clone(), prost_types_to_json(value));
    }
    serde_json::Value::Object(map)
}

fn prost_types_to_json(value: &prost_types::Value) -> serde_json::Value {
    use prost_types::value::Kind;
    match &value.kind {
        Some(Kind::NullValue(_)) => serde_json::Value::Null,
        Some(Kind::NumberValue(number)) => serde_json::json!(number),
        Some(Kind::StringValue(text)) => serde_json::json!(text),
        Some(Kind::BoolValue(flag)) => serde_json::json!(flag),
        Some(Kind::StructValue(fields)) => prost_types_to_json_fields(fields),
        Some(Kind::ListValue(list)) => serde_json::Value::Array(
            list.values
                .iter()
                .map(prost_types_to_json)
                .collect(),
        ),
        None => serde_json::Value::Null,
    }
}
