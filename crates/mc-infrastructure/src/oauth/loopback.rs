//! One-shot local HTTP listener that catches an OAuth2 authorization-code
//! redirect (`http://127.0.0.1:<port>/callback?code=...&state=...`).

use mc_domain::DomainError;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

/// The page the browser lands on after the provider redirects back.
fn page(ok: bool) -> String {
    let (mark, color, title, text) = if ok {
        ("✓", "#2f9e5b", "Login completato", "Puoi chiudere questa scheda e tornare a mail-cat.")
    } else {
        ("✕", "#d9534f", "Login non completato", "L'accesso è stato annullato o rifiutato. Chiudi questa scheda e riprova da mail-cat.")
    };
    format!(
        r#"<!doctype html><html lang="it"><head><meta charset="utf-8"><title>mail-cat</title>
<meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;font-family:system-ui,-apple-system,Segoe UI,sans-serif;background:#f4f5f7;color:#1f2328">
<style>@media (prefers-color-scheme:dark){{body{{background:#16181d!important;color:#d7dae0!important}}.card{{background:#1e2128!important;border-color:#2c3038!important}}}}</style>
<div class="card" style="text-align:center;padding:40px 48px;background:#fff;border:1px solid #e3e5e8;border-radius:14px;box-shadow:0 8px 30px rgba(0,0,0,.08);max-width:360px">
<div style="width:56px;height:56px;margin:0 auto 16px;border-radius:50%;background:{color};color:#fff;font-size:28px;line-height:56px">{mark}</div>
<div style="font-size:13px;letter-spacing:.08em;text-transform:uppercase;opacity:.6">mail-cat</div>
<h1 style="font-size:20px;margin:8px 0 10px">{title}</h1>
<p style="margin:0;opacity:.75;line-height:1.5">{text}</p>
</div></body></html>"#
    )
}

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

    let body = page(code.is_some());
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
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
                .write_all(
                    b"GET /callback?code=abc123&state=xyz789 HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
                )
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
