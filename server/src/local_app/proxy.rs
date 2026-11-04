//! Configures the local application proxy.
use std::{net::SocketAddr, sync::Arc};

use hudsucker::{
    certificate_authority::RcgenAuthority,
    hyper::{Request, Uri},
    rustls::crypto::aws_lc_rs,
    Body, HttpContext, HttpHandler, Proxy, RequestOrResponse,
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

use std::sync::OnceLock;

use prost::Message as _;

use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;

use crate::{
    api::cursor::proxy::UPSTREAM_URL_HEADER,
    cursor::{
        protocol::proto::runinference::v1 as ri,
        runinference,
        transport::TransportRegistry,
    },
    cursor::services::tab::is_tab_path, store::TabMode, Error, Result,
};

use super::ca::LoadedCa;

#[derive(Default)]
pub struct ProxyRuntime {
    url: Option<String>,
    port: Option<u16>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl ProxyRuntime {
    pub fn running(&self) -> bool {
        self.task.as_ref().is_some_and(|task| !task.is_finished())
    }
    pub fn url(&self) -> Option<String> {
        self.running().then(|| self.url.clone()).flatten()
    }
    pub fn port(&self) -> Option<u16> {
        if self.running() {
            self.port
        } else {
            None
        }
    }

    pub async fn start(
        &mut self,
        backend: SocketAddr,
        ca: LoadedCa,
        requested_port: u16,
        tab_mode: Arc<RwLock<TabMode>>,
    ) -> Result<(String, u16)> {
        if let Some(url) = self.url() {
            return Ok((url, self.port.unwrap_or_default()));
        }
        let listener = bind_proxy_listener(requested_port).await?;
        let address = listener.local_addr()?;
        let (stop, done) = oneshot::channel();
        let authority = RcgenAuthority::new(ca.issuer, 1_000, aws_lc_rs::default_provider());
        let proxy = Proxy::builder()
            .with_listener(listener)
            .with_ca(authority)
            .with_rustls_connector(aws_lc_rs::default_provider())
            .with_http_handler(CursorRelay { backend, tab_mode })
            .with_graceful_shutdown(async move {
                let _ = done.await;
            })
            .build()
            .map_err(|error| Error::Store(format!("build Cursor proxy: {error}")))?;
        self.stop = Some(stop);
        self.url = Some(format!("http://{address}"));
        self.port = Some(address.port());
        self.task = Some(tokio::spawn(async move {
            if let Err(error) = proxy.start().await {
                tracing::error!(%error, "Cursor proxy stopped unexpectedly");
            }
        }));
        Ok((self.url.clone().unwrap(), address.port()))
    }

    pub async fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), task).await;
        }
        self.url = None;
        self.port = None;
    }
}

async fn bind_proxy_listener(requested_port: u16) -> Result<TcpListener> {
    let requested = SocketAddr::from(([127, 0, 0, 1], requested_port));
    match TcpListener::bind(requested).await {
        Ok(listener) => Ok(listener),
        Err(error) if requested_port != 0 => {
            tracing::warn!(%requested, %error, "configured proxy port unavailable; selecting a random port");
            Ok(TcpListener::bind("127.0.0.1:0").await?)
        }
        Err(error) => Err(error.into()),
    }
}

#[derive(Clone)]
struct CursorRelay {
    backend: SocketAddr,
    tab_mode: Arc<RwLock<TabMode>>,
}

impl HttpHandler for CursorRelay {
    async fn handle_request(
        &mut self,
        _ctx: &HttpContext,
        mut request: Request<Body>,
    ) -> RequestOrResponse {
        let original = request.uri().clone();
        tracing::debug!(
            host = original.host().unwrap_or_default(),
            path = original.path(),
            method = %request.method(),
            "MITM request"
        );
        if original.path() == "/agent/v1/run" && request.method() == hudsucker::hyper::Method::GET
        {
            if let Some(response) = serve_agent_run_ws(_ctx.client_addr, &mut request) {
                return RequestOrResponse::Response(response);
            }
        }
        let locally_routed = should_route_locally(original.path(), *self.tab_mode.read());
        if is_cursor_host(original.host().unwrap_or_default()) && locally_routed {
            if let Ok(value) = original.to_string().parse() {
                request.headers_mut().insert(UPSTREAM_URL_HEADER, value);
            }
            let path = original
                .path_and_query()
                .map(|value| value.as_str())
                .unwrap_or("/");
            if let Ok(uri) = format!("http://{}{}", self.backend, path).parse::<Uri>() {
                *request.uri_mut() = uri;
            }
        }
        request.into()
    }

    async fn should_intercept_connect(
        &mut self,
        _ctx: &HttpContext,
        request: &Request<Body>,
    ) -> bool {
        request
            .uri()
            .authority()
            .is_some_and(|authority| is_cursor_host(authority.host()))
    }

    async fn should_intercept_tls(
        &mut self,
        _ctx: &HttpContext,
        hello: hudsucker::rustls::server::ClientHello<'_>,
    ) -> bool {
        hello.server_name().is_some_and(is_cursor_host)
    }
}

pub fn is_cursor_host(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    matches!(host.as_str(), "api2.cursor.sh" | "api3.cursor.sh") || host.ends_with(".cursor.sh")
}

fn is_local_path(path: &str) -> bool {
    matches!(
        path,
        "/agent.v1.AgentService/RunSSE"
            | "/aiserver.v1.BidiService/BidiAppend"
            | "/aiserver.v1.AiService/AvailableDocs"
            | "/aiserver.v1.DashboardService/GetEffectiveUserPlugins"
            | "/aiserver.v1.DashboardService/GetUserPrivacyMode"
            | "/agent.v1.AgentService/UpdateConversationMetadata"
            | "/aiserver.v1.AiService/GetServerConfig"
            | "/aiserver.v1.ServerConfigService/GetServerConfig"
            | "/aiserver.v1.AiService/AvailableModels"
            | "/agent.v1.AgentService/GetUsableModels"
            | "/aiserver.v1.AiService/GetUsableModels"
            | "/agent.v1.AgentService/GetDefaultModelForCli"
            | "/aiserver.v1.AiService/GetDefaultModelForCli"
            | "/aiserver.v1.AiService/GetDefaultModel"
            | "/aiserver.v1.AiService/GetDefaultModelNudgeData"
            | "/aiserver.v1.AuthService/GetEmail"
            | "/aiserver.v1.AuthService/GetUserMeta"
            | "/aiserver.v1.DashboardService/GetMe"
            | "/aiserver.v1.DashboardService/GetTeams"
            | "/aiserver.v1.DashboardService/GetUserProfile"
            | "/aiserver.v1.DashboardService/GetCurrentPeriodUsage"
            | "/aiserver.v1.DashboardService/GetUsageLimitStatusAndActiveGrants"
            | "/aiserver.v1.AiService/KnowledgeBaseAdd"
            | "/aiserver.v1.AiService/KnowledgeBaseList"
            | "/aiserver.v1.AiService/KnowledgeBaseUpdate"
            | "/aiserver.v1.AiService/KnowledgeBaseRemove"
            | "/aiserver.v1.AiService/WriteGitCommitMessage"
            | "/aiserver.v1.NetworkService/IsConnected"
            | "/aiserver.v1.AnalyticsService/BootstrapStatsig"
            | "/auth/full_stripe_profile"
            | "/auth/stripe_profile"
    )
}

fn should_route_locally(path: &str, tab_mode: TabMode) -> bool {
    is_local_path(path) || (is_tab_path(path) && tab_mode != TabMode::Direct)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_cli_transport_and_model_metadata_routes_stay_local() {
        for path in [
            "/aiserver.v1.AiService/GetServerConfig",
            "/aiserver.v1.ServerConfigService/GetServerConfig",
            "/agent.v1.AgentService/GetDefaultModelForCli",
            "/aiserver.v1.AiService/GetDefaultModelForCli",
            "/aiserver.v1.AiService/GetDefaultModel",
            "/aiserver.v1.AiService/GetDefaultModelNudgeData",
            "/aiserver.v1.AiService/AvailableDocs",
            "/aiserver.v1.DashboardService/GetEffectiveUserPlugins",
            "/aiserver.v1.DashboardService/GetUserPrivacyMode",
            "/aiserver.v1.AuthService/GetUserMeta",
            "/agent.v1.AgentService/UpdateConversationMetadata",
            "/auth/full_stripe_profile",
            "/auth/stripe_profile",
        ] {
            assert!(is_local_path(path), "{path} must not reach Cursor upstream");
        }
    }
}


static RUN_REGISTRY: OnceLock<TransportRegistry> = OnceLock::new();

/// Shares the transport registry with the MITM WebSocket bridge. The registry
/// is created after the harness in app startup, so it is wired in once ready.
pub fn set_run_registry(registry: TransportRegistry) {
    let _ = RUN_REGISTRY.set(registry);
}

/// Serves Cursor's RunInference WebSocket runs (wss://api2.cursor.sh/agent/v1/run,
/// subprotocol cursor-run-inference-preconnect-v1) straight from the local
/// engine. The upgrade is answered locally because hudsucker's built-in
/// WebSocket path drops the subprotocol from the 101, which makes the harness
/// client close the socket immediately.
const AGENT_RUN_WS_PROTOCOL: &str = "cursor-run-inference-preconnect-v1";

fn ws_session_key(addr: SocketAddr) -> String {
    format!("ws-{addr}")
}

/// Strips a connect 5-byte envelope (flags + BE length) when present.
fn strip_envelope(data: &[u8]) -> &[u8] {
    if data.len() > 5 && data[0] & 0xFE == 0 {
        let length = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
        if length == data.len() - 5 {
            return &data[5..];
        }
    }
    data
}

fn serve_agent_run_ws(
    client_addr: SocketAddr,
    request: &mut Request<Body>,
) -> Option<hudsucker::hyper::Response<Body>> {
    use hudsucker::hyper::{
        header::{HeaderValue, CONNECTION, SEC_WEBSOCKET_PROTOCOL, UPGRADE},
        upgrade::OnUpgrade,
        StatusCode,
    };
    let on_upgrade = request.extensions_mut().remove::<OnUpgrade>()?;
    let key = request
        .headers()
        .get("sec-websocket-key")?
        .to_str()
        .ok()?
        .to_owned();
    let mut response = hudsucker::hyper::Response::new(Body::empty());
    *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    let headers = response.headers_mut();
    headers.insert(UPGRADE, HeaderValue::from_static("websocket"));
    headers.insert(CONNECTION, HeaderValue::from_static("Upgrade"));
    headers.insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(AGENT_RUN_WS_PROTOCOL),
    );
    headers.insert("sec-websocket-accept", ws_accept_key(&key));
    let registry = RUN_REGISTRY.get().cloned()?;
    tokio::spawn(bridge_agent_run_ws(
        registry,
        ws_session_key(client_addr),
        on_upgrade,
    ));
    Some(response)
}

fn ws_accept_key(key: &str) -> hudsucker::hyper::header::HeaderValue {
    use base64::Engine;
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());
    hasher.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    hudsucker::hyper::header::HeaderValue::from_str(
        &base64::engine::general_purpose::STANDARD.encode(hasher.finalize()),
    )
    .expect("base64 accept key is a valid header value")
}

async fn bridge_agent_run_ws(
    registry: TransportRegistry,
    _key: String,
    upgraded: hudsucker::hyper::upgrade::OnUpgrade,
) {
    use tokio_tungstenite::{tungstenite::protocol::Role, WebSocketStream};

    use crate::cursor::protocol::proto::runinference::v1 as ri;

    let Ok(upgraded) = upgraded.await else {
        return;
    };
    let ws = WebSocketStream::from_raw_socket(
        hudsucker::hyper_util::rt::TokioIo::new(upgraded),
        Role::Server,
        None,
    )
    .await;
    let (mut sink, mut stream) = ws.split();
    let (outbound, mut outbound_rx) =
        tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let writer = tokio::spawn(async move {
        while let Some(bytes) = outbound_rx.recv().await {
            if sink
                .send(tokio_tungstenite::tungstenite::Message::Binary(
                    bytes::Bytes::from(bytes),
                ))
                .await
                .is_err()
            {
                break;
            }
        }
    });
    tracing::info!("RunInference WS session opened");
    let mut run_request_ids: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    while let Some(Ok(message)) = stream.next().await {
        let tokio_tungstenite::tungstenite::Message::Binary(data) = message else {
            continue;
        };
        let frame = match ri::AgentWebSocketClientFrame::decode(strip_envelope(&data)) {
            Ok(frame) => frame,
            Err(error) => {
                tracing::warn!(%error, "undecodable AgentWebSocket client frame");
                continue;
            }
        };
        match frame.frame {
            Some(ri::agent_web_socket_client_frame::Frame::Hello(hello)) => {
                let reply = ri::AgentWebSocketServerFrame {
                    frame: Some(ri::agent_web_socket_server_frame::Frame::Hello(
                        ri::AgentWebSocketHello {
                            protocol_version: hello.protocol_version,
                        },
                    )),
                };
                let _ = outbound.send(reply.encode_to_vec());
            }
            Some(ri::agent_web_socket_client_frame::Frame::RunStart(start)) => {
                let Some(identity) = start.identity.clone() else {
                    continue;
                };
                let run_id = identity.run_id.clone();
                let request_id = identity.request_id.clone();
                tracing::info!(run_id = %run_id, request_id = %request_id, "RunInference WS run start");
                run_request_ids.insert(run_id.clone(), request_id.clone());
                let reply = ri::AgentWebSocketServerFrame {
                    frame: Some(ri::agent_web_socket_server_frame::Frame::RunAccepted(
                        ri::AgentWebSocketRunAccepted {
                            identity: Some(identity),
                            headers: Vec::new(),
                        },
                    )),
                };
                let _ = outbound.send(reply.encode_to_vec());
                if let Ok(handle) = registry.get_or_create(&request_id).await {
                    let mut receiver = handle.subscribe();
                    let outbound = outbound.clone();
                    let run_id = run_id.to_owned();
                    tokio::spawn(async move {
                        let mut sequence = 0u64;
                        while let Some(frame) = receiver.recv().await {
                            let payload = strip_envelope(&frame).to_vec();
                            let message = ri::AgentWebSocketServerFrame {
                                frame: Some(
                                    ri::agent_web_socket_server_frame::Frame::ServerMessage(
                                        ri::AgentWebSocketServerMessage {
                                            run_id: run_id.clone(),
                                            sequence,
                                            message: payload,
                                        },
                                    ),
                                ),
                            };
                            sequence += 1;
                            if outbound.send(message.encode_to_vec()).is_err() {
                                break;
                            }
                        }
                    });
                }
            }
            Some(ri::agent_web_socket_client_frame::Frame::ClientMessage(client)) => {
                let Some(request_id) = run_request_ids
                    .get(&client.run_id)
                    .map(String::as_str)
                    .map(str::to_owned)
                else {
                    continue;
                };
                let payload = strip_envelope(&client.message);
                match ri::RunInferenceClientMessage::decode(payload) {
                    Ok(message) => {
                        let _ = runinference::handle_append(
                            &registry,
                            &request_id,
                            client.sequence as i64,
                            message,
                        )
                        .await;
                    }
                    Err(error) => {
                        tracing::warn!(%error, "undecodable RunInference WS message");
                    }
                }
            }
            Some(ri::agent_web_socket_client_frame::Frame::CancelRun(cancel)) => {
                if let Some(request_id) = run_request_ids.get(&cancel.run_id) {
                    runinference::cancel_run(&registry, request_id).await;
                }
            }
            Some(ri::agent_web_socket_client_frame::Frame::HalfClose(_)) => {}
            None => {}
        }
    }
    tracing::info!("RunInference WS session closed");
    writer.abort();
}

