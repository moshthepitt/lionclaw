use std::{collections::BTreeSet, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use tokio::{
    io::{copy_bidirectional, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tracing::debug;

use crate::model::{Destination, NetworkGrant};

const MAX_HTTP_HEADER_BYTES: usize = 8 * 1024;
const SOCKS_VERSION: u8 = 5;
const SOCKS_CMD_CONNECT: u8 = 1;
const SOCKS_ATYP_IPV4: u8 = 1;
const SOCKS_ATYP_DOMAIN: u8 = 3;
const SOCKS_ATYP_IPV6: u8 = 4;

pub async fn run(http: String, socks: String, allow: Vec<String>) -> Result<()> {
    let destinations = Arc::new(parse_allowlist(allow)?);
    let http = TcpListener::bind(&http)
        .await
        .with_context(|| format!("binding HTTP CONNECT proxy on {http}"))?;
    let socks = TcpListener::bind(&socks)
        .await
        .with_context(|| format!("binding SOCKS proxy on {socks}"))?;

    tokio::select! {
        result = serve_http(http, Arc::clone(&destinations)) => result,
        result = serve_socks(socks, destinations) => result,
    }
}

pub async fn health(http: String, socks: String) -> Result<()> {
    for address in [http, socks] {
        timeout(Duration::from_secs(1), TcpStream::connect(&address))
            .await
            .with_context(|| format!("timed out connecting to proxy listener {address}"))?
            .with_context(|| format!("connecting to proxy listener {address}"))?;
    }
    Ok(())
}

fn parse_allowlist(raw: Vec<String>) -> Result<NetworkGrant> {
    if raw.is_empty() {
        bail!("network proxy requires at least one --allow HOST:PORT destination");
    }
    let destinations = raw
        .iter()
        .map(|destination| parse_destination(destination))
        .collect::<Result<BTreeSet<_>>>()?;
    NetworkGrant::allow(destinations).context("network proxy destination grant is invalid")
}

fn parse_destination(raw: &str) -> Result<Destination> {
    let (host, port) = raw
        .rsplit_once(':')
        .with_context(|| format!("destination '{raw}' must be HOST:PORT"))?;
    let port = port
        .parse::<u16>()
        .with_context(|| format!("destination '{raw}' has an invalid port"))?;
    Destination::single(host, port).with_context(|| format!("destination '{raw}' is invalid"))
}

async fn serve_http(listener: TcpListener, destinations: Arc<NetworkGrant>) -> Result<()> {
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .context("accepting HTTP proxy connection")?;
        let destinations = Arc::clone(&destinations);
        tokio::spawn(async move {
            if let Err(error) = handle_http(stream, destinations).await {
                debug!(?error, "HTTP proxy connection closed");
            }
        });
    }
}

async fn handle_http(mut client: TcpStream, destinations: Arc<NetworkGrant>) -> Result<()> {
    let request = match read_http_request_head(&mut client).await {
        Ok(request) => request,
        Err(error) => {
            write_http_status(&mut client, 400, "Bad Request").await;
            return Err(error);
        }
    };
    let (first_line, rest) = split_http_first_line(&request.header)?;
    let mut fields = first_line.split_whitespace();
    let method = fields
        .next()
        .ok_or_else(|| anyhow!("empty HTTP request line"))?;
    let target = fields
        .next()
        .ok_or_else(|| anyhow!("missing HTTP request target"))?;
    let version = fields
        .next()
        .ok_or_else(|| anyhow!("missing HTTP request version"))?;
    if fields.next().is_some() || !version.starts_with("HTTP/") {
        write_http_status(&mut client, 400, "Bad Request").await;
        bail!("malformed HTTP request line");
    }

    if method.eq_ignore_ascii_case("CONNECT") {
        let (host, port) = match parse_required_host_port(target) {
            Ok(destination) => destination,
            Err(error) => {
                write_http_status(&mut client, 400, "Bad Request").await;
                return Err(error);
            }
        };
        if !destination_allowed(&destinations, &host, port) {
            write_http_status(&mut client, 403, "Forbidden").await;
            bail!("CONNECT destination is not declared");
        }
        let mut upstream = TcpStream::connect((host.as_str(), port))
            .await
            .with_context(|| format!("connecting to declared destination {host}:{port}"))?;
        client
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await
            .context("writing CONNECT success")?;
        if !request.buffered_body.is_empty() {
            upstream
                .write_all(&request.buffered_body)
                .await
                .context("forwarding buffered CONNECT payload")?;
        }
        let _ = copy_bidirectional(&mut client, &mut upstream).await;
        return Ok(());
    }

    let (host, port, path) = match parse_absolute_http_target(target) {
        Ok(destination) => destination,
        Err(error) => {
            write_http_status(&mut client, 400, "Bad Request").await;
            return Err(error);
        }
    };
    if !destination_allowed(&destinations, &host, port) {
        write_http_status(&mut client, 403, "Forbidden").await;
        bail!("HTTP destination is not declared");
    }
    let mut upstream = TcpStream::connect((host.as_str(), port))
        .await
        .with_context(|| format!("connecting to declared destination {host}:{port}"))?;
    upstream
        .write_all(format!("{method} {path} {version}").as_bytes())
        .await
        .context("forwarding HTTP request line")?;
    upstream
        .write_all(rest)
        .await
        .context("forwarding HTTP headers")?;
    if !request.buffered_body.is_empty() {
        upstream
            .write_all(&request.buffered_body)
            .await
            .context("forwarding buffered HTTP request body")?;
    }
    let _ = copy_bidirectional(&mut client, &mut upstream).await;
    Ok(())
}

struct BufferedHttpRequest {
    header: Vec<u8>,
    buffered_body: Vec<u8>,
}

async fn read_http_request_head(stream: &mut TcpStream) -> Result<BufferedHttpRequest> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = stream
            .read(&mut chunk)
            .await
            .context("reading HTTP proxy request")?;
        if read == 0 {
            bail!("HTTP proxy request ended before headers");
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(position) = find_header_end(&buffer) {
            let body_start = position + 4;
            let buffered_body = buffer.split_off(body_start);
            return Ok(BufferedHttpRequest {
                header: buffer,
                buffered_body,
            });
        }
        if buffer.len() > MAX_HTTP_HEADER_BYTES {
            bail!("HTTP proxy request headers exceed {MAX_HTTP_HEADER_BYTES} bytes");
        }
    }
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn split_http_first_line(header: &[u8]) -> Result<(&str, &[u8])> {
    let line_end = header
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or_else(|| anyhow!("HTTP request has no first-line terminator"))?;
    let first_line = std::str::from_utf8(&header[..line_end]).context("HTTP request line UTF-8")?;
    Ok((first_line, &header[line_end..]))
}

fn parse_required_host_port(target: &str) -> Result<(String, u16)> {
    parse_authority(target, None)
}

fn parse_absolute_http_target(target: &str) -> Result<(String, u16, String)> {
    let suffix = target
        .strip_prefix("http://")
        .ok_or_else(|| anyhow!("HTTP proxy request target must be absolute http:// URL"))?;
    let (authority, path) = match suffix.find('/') {
        Some(index) => (&suffix[..index], &suffix[index..]),
        None => (suffix, "/"),
    };
    let (host, port) = parse_authority(authority, Some(80))?;
    Ok((host, port, path.to_string()))
}

fn parse_authority(authority: &str, default_port: Option<u16>) -> Result<(String, u16)> {
    if authority.is_empty() || authority.contains('@') || authority.starts_with('[') {
        bail!("network authority must be a DNS host with a declared port");
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port))
            if !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            let port = port
                .parse::<u16>()
                .with_context(|| format!("network authority '{authority}' has an invalid port"))?;
            (host, port)
        }
        Some(_) => bail!("network authority '{authority}' has an invalid port"),
        None => {
            let port = default_port
                .ok_or_else(|| anyhow!("network authority '{authority}' must include a port"))?;
            (authority, port)
        }
    };
    if host.is_empty() {
        bail!("network authority must include a host");
    }
    Ok((host.to_string(), port))
}

fn destination_allowed(destinations: &NetworkGrant, host: &str, port: u16) -> bool {
    destinations.allows(host, port)
}

async fn write_http_status(stream: &mut TcpStream, code: u16, reason: &str) {
    let _ = stream
        .write_all(format!("HTTP/1.1 {code} {reason}\r\nContent-Length: 0\r\n\r\n").as_bytes())
        .await;
}

async fn serve_socks(listener: TcpListener, destinations: Arc<NetworkGrant>) -> Result<()> {
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .context("accepting SOCKS proxy connection")?;
        let destinations = Arc::clone(&destinations);
        tokio::spawn(async move {
            if let Err(error) = handle_socks(stream, destinations).await {
                debug!(?error, "SOCKS proxy connection closed");
            }
        });
    }
}

async fn handle_socks(mut client: TcpStream, destinations: Arc<NetworkGrant>) -> Result<()> {
    let mut greeting = [0_u8; 2];
    client
        .read_exact(&mut greeting)
        .await
        .context("reading SOCKS greeting")?;
    if greeting[0] != SOCKS_VERSION {
        bail!("unsupported SOCKS version");
    }
    let mut methods = vec![0_u8; greeting[1] as usize];
    client
        .read_exact(&mut methods)
        .await
        .context("reading SOCKS methods")?;
    if !methods.contains(&0) {
        client.write_all(&[SOCKS_VERSION, 0xff]).await.ok();
        bail!("SOCKS client did not offer no-auth method");
    }
    client
        .write_all(&[SOCKS_VERSION, 0])
        .await
        .context("selecting SOCKS no-auth method")?;

    let mut request = [0_u8; 4];
    client
        .read_exact(&mut request)
        .await
        .context("reading SOCKS request")?;
    if request[0] != SOCKS_VERSION || request[1] != SOCKS_CMD_CONNECT || request[2] != 0 {
        write_socks_reply(&mut client, 7).await;
        bail!("unsupported SOCKS request");
    }
    let (host, port) = match request[3] {
        SOCKS_ATYP_DOMAIN => read_socks_domain_destination(&mut client).await?,
        SOCKS_ATYP_IPV4 => {
            drain_socks_ip_destination(&mut client, 4).await?;
            write_socks_reply(&mut client, 8).await;
            bail!("SOCKS IP-literal destination is not allowed");
        }
        SOCKS_ATYP_IPV6 => {
            drain_socks_ip_destination(&mut client, 16).await?;
            write_socks_reply(&mut client, 8).await;
            bail!("SOCKS IP-literal destination is not allowed");
        }
        _ => {
            write_socks_reply(&mut client, 8).await;
            bail!("unsupported SOCKS address type");
        }
    };
    if !destination_allowed(&destinations, &host, port) {
        write_socks_reply(&mut client, 2).await;
        bail!("SOCKS destination is not declared");
    }
    let mut upstream = match TcpStream::connect((host.as_str(), port)).await {
        Ok(upstream) => upstream,
        Err(error) => {
            write_socks_reply(&mut client, 5).await;
            return Err(error)
                .with_context(|| format!("connecting to declared destination {host}:{port}"));
        }
    };
    write_socks_reply(&mut client, 0).await;
    let _ = copy_bidirectional(&mut client, &mut upstream).await;
    Ok(())
}

async fn read_socks_domain_destination(stream: &mut TcpStream) -> Result<(String, u16)> {
    let mut length = [0_u8; 1];
    stream
        .read_exact(&mut length)
        .await
        .context("reading SOCKS domain length")?;
    if length[0] == 0 {
        bail!("SOCKS domain destination is empty");
    }
    let mut host = vec![0_u8; length[0] as usize];
    stream
        .read_exact(&mut host)
        .await
        .context("reading SOCKS domain destination")?;
    let host = String::from_utf8(host).context("SOCKS domain destination UTF-8")?;
    let port = read_socks_port(stream).await?;
    Ok((host, port))
}

async fn drain_socks_ip_destination(stream: &mut TcpStream, bytes: usize) -> Result<()> {
    let mut ignored = vec![0_u8; bytes];
    stream
        .read_exact(&mut ignored)
        .await
        .context("reading SOCKS IP destination")?;
    let _ = read_socks_port(stream).await?;
    Ok(())
}

async fn read_socks_port(stream: &mut TcpStream) -> Result<u16> {
    let mut raw = [0_u8; 2];
    stream
        .read_exact(&mut raw)
        .await
        .context("reading SOCKS destination port")?;
    Ok(u16::from_be_bytes(raw))
}

async fn write_socks_reply(stream: &mut TcpStream, code: u8) {
    let _ = stream
        .write_all(&[SOCKS_VERSION, code, 0, SOCKS_ATYP_IPV4, 0, 0, 0, 0, 0, 0])
        .await;
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{task::JoinHandle, time::timeout};

    use super::*;

    #[tokio::test]
    async fn http_connect_allows_declared_destination_only() {
        let upstream = spawn_response_server(b"ok").await;
        let proxy =
            spawn_http_proxy(NetworkGrant::allow_single("localhost", upstream.port).unwrap()).await;

        let mut client = TcpStream::connect(proxy.addr).await.expect("proxy");
        client
            .write_all(format!("CONNECT localhost:{} HTTP/1.1\r\n\r\n", upstream.port).as_bytes())
            .await
            .expect("connect request");
        let response = read_proxy_response(&mut client).await;
        assert!(response.starts_with("HTTP/1.1 200"));
        let mut body = [0_u8; 2];
        client.read_exact(&mut body).await.expect("body");
        assert_eq!(&body, b"ok");

        let denied = send_connect(proxy.addr, &format!("localhost:{}", upstream.port + 1)).await;
        assert!(denied.starts_with("HTTP/1.1 403"));
        let denied = send_connect(proxy.addr, &format!("denied.localhost:{}", upstream.port)).await;
        assert!(denied.starts_with("HTTP/1.1 403"));
        let denied = send_connect(proxy.addr, &format!("127.0.0.1:{}", upstream.port)).await;
        assert!(denied.starts_with("HTTP/1.1 403"));

        proxy.task.abort();
        upstream.task.abort();
    }

    #[tokio::test]
    async fn http_redirect_to_undeclared_destination_is_rejected() {
        let denied = spawn_response_server(b"denied").await;
        let redirect = spawn_response_server(
            format!(
                "HTTP/1.1 302 Found\r\nLocation: http://localhost:{}/\r\nContent-Length: 0\r\n\r\n",
                denied.port
            )
            .into_bytes(),
        )
        .await;
        let proxy =
            spawn_http_proxy(NetworkGrant::allow_single("localhost", redirect.port).unwrap()).await;

        let response =
            send_absolute_get(proxy.addr, &format!("http://localhost:{}/", redirect.port)).await;
        assert!(response.starts_with("HTTP/1.1 302"));
        let response =
            send_absolute_get(proxy.addr, &format!("http://localhost:{}/", denied.port)).await;
        assert!(response.starts_with("HTTP/1.1 403"));

        proxy.task.abort();
        redirect.task.abort();
        denied.task.abort();
    }

    #[tokio::test]
    async fn socks_uses_same_destination_predicate_and_rejects_dns_bypass() {
        let upstream = spawn_response_server(b"ok").await;
        let proxy =
            spawn_socks_proxy(NetworkGrant::allow_single("localhost", upstream.port).unwrap())
                .await;

        let mut client = TcpStream::connect(proxy.addr).await.expect("proxy");
        socks_greet(&mut client).await;
        socks_connect_domain(&mut client, "localhost", upstream.port).await;
        let mut body = [0_u8; 2];
        client.read_exact(&mut body).await.expect("body");
        assert_eq!(&body, b"ok");

        let code = socks_connect_domain_once(proxy.addr, "localhost", upstream.port + 1).await;
        assert_eq!(code, 2);
        let code = socks_connect_ipv4_once(proxy.addr, upstream.port).await;
        assert_eq!(code, 8);

        proxy.task.abort();
        upstream.task.abort();
    }

    #[tokio::test]
    async fn malformed_hosts_do_not_reach_dns() {
        let proxy = spawn_http_proxy(NetworkGrant::allow_single("localhost", 443).unwrap()).await;
        let response = send_connect(proxy.addr, "localhost").await;
        assert!(response.starts_with("HTTP/1.1 400"));
        let response = send_connect(proxy.addr, "[::1]:443").await;
        assert!(response.starts_with("HTTP/1.1 400"));
        proxy.task.abort();
    }

    #[tokio::test]
    async fn http_proxy_preserves_body_bytes_buffered_with_headers() {
        let upstream = spawn_body_capture_server(11).await;
        let proxy =
            spawn_http_proxy(NetworkGrant::allow_single("localhost", upstream.port).unwrap()).await;

        let mut client = TcpStream::connect(proxy.addr).await.expect("proxy");
        client
            .write_all(
                format!(
                    "POST http://localhost:{}/upload HTTP/1.1\r\nHost: localhost:{}\r\nContent-Length: 11\r\n\r\nhello world",
                    upstream.port, upstream.port
                )
                .as_bytes(),
            )
            .await
            .expect("post request");
        let response = read_proxy_response(&mut client).await;
        assert!(response.starts_with("HTTP/1.1 200"));

        let body = upstream
            .body
            .await
            .expect("body captured by upstream server");
        assert_eq!(body, b"hello world");

        proxy.task.abort();
        upstream.task.abort();
    }

    struct Server {
        port: u16,
        task: JoinHandle<()>,
    }

    struct BodyCaptureServer {
        port: u16,
        task: JoinHandle<()>,
        body: tokio::sync::oneshot::Receiver<Vec<u8>>,
    }

    struct Proxy {
        addr: std::net::SocketAddr,
        task: JoinHandle<Result<()>>,
    }

    async fn spawn_response_server(response: impl Into<Vec<u8>>) -> Server {
        let response = Arc::new(response.into());
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
        let port = listener.local_addr().expect("server addr").port();
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.expect("accept server");
                let response = Arc::clone(&response);
                tokio::spawn(async move {
                    let mut scratch = [0_u8; 1024];
                    let _ = timeout(Duration::from_millis(50), stream.read(&mut scratch)).await;
                    stream.write_all(&response).await.ok();
                });
            }
        });
        Server { port, task }
    }

    async fn spawn_body_capture_server(expected_len: usize) -> BodyCaptureServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
        let port = listener.local_addr().expect("server addr").port();
        let (body_tx, body) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept server");
            let mut buffer = Vec::new();
            let mut chunk = [0_u8; 1024];
            let body = loop {
                let read = timeout(Duration::from_millis(200), stream.read(&mut chunk))
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .unwrap_or(0);
                if read == 0 {
                    break Vec::new();
                }
                buffer.extend_from_slice(&chunk[..read]);
                if let Some(position) = find_header_end(&buffer) {
                    let mut body = buffer[(position + 4)..].to_vec();
                    while body.len() < expected_len {
                        let read = timeout(Duration::from_millis(200), stream.read(&mut chunk))
                            .await
                            .ok()
                            .and_then(Result::ok)
                            .unwrap_or(0);
                        if read == 0 {
                            break;
                        }
                        body.extend_from_slice(&chunk[..read]);
                    }
                    body.truncate(expected_len);
                    break body;
                }
            };
            let _ = body_tx.send(body);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await
                .ok();
        });
        BodyCaptureServer { port, task, body }
    }

    async fn spawn_http_proxy(grant: NetworkGrant) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let addr = listener.local_addr().expect("proxy addr");
        let task = tokio::spawn(serve_http(listener, Arc::new(grant)));
        Proxy { addr, task }
    }

    async fn spawn_socks_proxy(grant: NetworkGrant) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let addr = listener.local_addr().expect("proxy addr");
        let task = tokio::spawn(serve_socks(listener, Arc::new(grant)));
        Proxy { addr, task }
    }

    async fn send_connect(addr: std::net::SocketAddr, target: &str) -> String {
        let mut client = TcpStream::connect(addr).await.expect("proxy");
        client
            .write_all(format!("CONNECT {target} HTTP/1.1\r\n\r\n").as_bytes())
            .await
            .expect("connect request");
        read_proxy_response(&mut client).await
    }

    async fn send_absolute_get(addr: std::net::SocketAddr, target: &str) -> String {
        let mut client = TcpStream::connect(addr).await.expect("proxy");
        client
            .write_all(format!("GET {target} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
            .await
            .expect("get request");
        read_proxy_response(&mut client).await
    }

    async fn read_proxy_response(stream: &mut TcpStream) -> String {
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let read = timeout(Duration::from_secs(2), stream.read(&mut chunk))
                .await
                .expect("timed response")
                .expect("read response");
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);
            if find_header_end(&buffer).is_some() {
                break;
            }
        }
        String::from_utf8_lossy(&buffer).to_string()
    }

    async fn socks_greet(stream: &mut TcpStream) {
        stream
            .write_all(&[SOCKS_VERSION, 1, 0])
            .await
            .expect("greet");
        let mut reply = [0_u8; 2];
        stream.read_exact(&mut reply).await.expect("greet reply");
        assert_eq!(reply, [SOCKS_VERSION, 0]);
    }

    async fn socks_connect_domain(stream: &mut TcpStream, host: &str, port: u16) {
        let mut request = vec![
            SOCKS_VERSION,
            SOCKS_CMD_CONNECT,
            0,
            SOCKS_ATYP_DOMAIN,
            host.len() as u8,
        ];
        request.extend_from_slice(host.as_bytes());
        request.extend_from_slice(&port.to_be_bytes());
        stream.write_all(&request).await.expect("socks request");
        let mut reply = [0_u8; 10];
        stream.read_exact(&mut reply).await.expect("socks reply");
        assert_eq!(reply[1], 0);
    }

    async fn socks_connect_domain_once(addr: std::net::SocketAddr, host: &str, port: u16) -> u8 {
        let mut client = TcpStream::connect(addr).await.expect("proxy");
        socks_greet(&mut client).await;
        let mut request = vec![
            SOCKS_VERSION,
            SOCKS_CMD_CONNECT,
            0,
            SOCKS_ATYP_DOMAIN,
            host.len() as u8,
        ];
        request.extend_from_slice(host.as_bytes());
        request.extend_from_slice(&port.to_be_bytes());
        client.write_all(&request).await.expect("socks request");
        let mut reply = [0_u8; 10];
        client.read_exact(&mut reply).await.expect("socks reply");
        reply[1]
    }

    async fn socks_connect_ipv4_once(addr: std::net::SocketAddr, port: u16) -> u8 {
        let mut client = TcpStream::connect(addr).await.expect("proxy");
        socks_greet(&mut client).await;
        let mut request = vec![
            SOCKS_VERSION,
            SOCKS_CMD_CONNECT,
            0,
            SOCKS_ATYP_IPV4,
            127,
            0,
            0,
            1,
        ];
        request.extend_from_slice(&port.to_be_bytes());
        client.write_all(&request).await.expect("socks request");
        let mut reply = [0_u8; 10];
        client.read_exact(&mut reply).await.expect("socks reply");
        reply[1]
    }
}
