//! One-shot local HTTP listener that catches an OAuth2 authorization-code
//! redirect (`http://127.0.0.1:<port>/callback?code=...&state=...`).

use mc_domain::DomainError;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

const SUCCESS_BODY: &str =
    "<html><body><h1>mail-cat</h1><p>Login completato, puoi chiudere questa scheda.</p></body></html>";

/// Accepts exactly one HTTP GET request on `listener`, parses `code`/`state`
/// from its query string, and replies with a static confirmation page.
pub async fn accept_one_redirect(listener: &TcpListener) -> Result<(String, String), DomainError> {
    let (stream, _) = listener
        .accept()
        .await
        .map_err(|e| DomainError::Provider(format!("OAuth loopback accept failed: {e}")))?;

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut request_line = String::new();
    reader
        .read_line(&mut request_line)
        .await
        .map_err(|e| DomainError::Provider(format!("OAuth loopback read failed: {e}")))?;

    let path = request_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| DomainError::Provider("malformed OAuth redirect request".into()))?;
    let query = path
        .split_once('?')
        .map(|(_, q)| q)
        .ok_or_else(|| DomainError::Provider("OAuth redirect had no query string".into()))?;

    let mut code = None;
    let mut state = None;
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "code" => code = Some(value.into_owned()),
            "state" => state = Some(value.into_owned()),
            _ => {}
        }
    }

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        SUCCESS_BODY.len(),
        SUCCESS_BODY
    );
    let _ = write_half.write_all(response.as_bytes()).await;
    let _ = write_half.flush().await;

    Ok((
        code.ok_or_else(|| DomainError::Provider("OAuth redirect missing 'code'".into()))?,
        state.ok_or_else(|| DomainError::Provider("OAuth redirect missing 'state'".into()))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn parses_code_and_state_from_redirect_query_string() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(b"GET /callback?code=abc123&state=xyz789 HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
                .await
                .unwrap();
            let mut buf = String::new();
            stream.read_to_string(&mut buf).await.ok();
            buf
        });

        let (code, state) = accept_one_redirect(&listener).await.unwrap();
        assert_eq!(code, "abc123");
        assert_eq!(state, "xyz789");

        let response = client.await.unwrap();
        assert!(response.contains("200 OK"));
    }
}
