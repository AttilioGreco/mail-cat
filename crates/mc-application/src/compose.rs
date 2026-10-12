//! Compose defaults and message assembly, modelled on Apple Mail: who a reply
//! goes to, how the original is quoted, how markdown becomes the MIME bodies,
//! and how a draft remembers what it was. Pure functions — the I/O around them
//! lives in `MessageCommandHandler`.

use std::collections::HashSet;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, Datelike, TimeZone};
use mailparse::{DispositionType, MailAddr, MailHeader, MailHeaderMap, ParsedMail};
use mc_domain::{Account, AccountId, Address, EmailAddress, Envelope, MessageId, OutgoingAttachment};
use pulldown_cmark::{Event, Options, Parser};

use crate::error::ApplicationError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComposeMode {
    New,
    Reply,
    ReplyAll,
    Forward,
    Draft,
}

impl ComposeMode {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "new" => Self::New,
            "reply" => Self::Reply,
            "replyAll" => Self::ReplyAll,
            "forward" => Self::Forward,
            "draft" => Self::Draft,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Reply => "reply",
            Self::ReplyAll => "replyAll",
            Self::Forward => "forward",
            Self::Draft => "draft",
        }
    }

    pub fn is_reply(self) -> bool {
        matches!(self, Self::Reply | Self::ReplyAll)
    }

    /// Modes whose body carries the original message below the user's text.
    pub fn quotes_source(self) -> bool {
        matches!(self, Self::Reply | Self::ReplyAll | Self::Forward)
    }
}

#[derive(Debug, Clone)]
pub struct ComposeIdentity {
    pub account_id: AccountId,
    pub display_name: String,
    pub email: String,
    pub signature_html: Option<String>,
}

/// An attachment of an existing message (the forwarded original or a stored
/// draft) the user may keep. `id` is `<message id>:<content fingerprint>`.
#[derive(Debug, Clone)]
pub struct ForwardedAttachment {
    pub id: String,
    pub filename: String,
    pub size: u64,
    pub mime_type: String,
}

/// Everything the compose window starts from.
#[derive(Debug, Clone)]
pub struct ComposeView {
    pub draft_id: Option<MessageId>,
    pub account_id: AccountId,
    pub identities: Vec<ComposeIdentity>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub subject: String,
    pub body_md: String,
    pub quoted_html: Option<String>,
    pub signature_html: Option<String>,
    pub forwarded_attachments: Vec<ForwardedAttachment>,
    /// A restored draft's images shown inline (`cid:` in `body_md` or the
    /// quote), so the editor can display them; empty otherwise.
    pub inline_images: Vec<OutgoingAttachment>,
    /// For a restored draft this is the mode it was started with, so the
    /// quote and threading are rebuilt on send.
    pub mode: ComposeMode,
    pub source_message_id: Option<MessageId>,
}

/// What the compose window hands back on send / save.
#[derive(Debug, Clone)]
pub struct ComposeInput {
    pub draft_id: Option<MessageId>,
    pub account_id: AccountId,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub subject: String,
    pub body_md: String,
    pub mode: ComposeMode,
    pub source_message_id: Option<MessageId>,
    pub attachments: Vec<OutgoingAttachment>,
    pub forwarded_attachment_ids: Vec<String>,
}

pub fn identities(accounts: &[Account]) -> Vec<ComposeIdentity> {
    accounts
        .iter()
        .map(|a| ComposeIdentity {
            account_id: a.id,
            display_name: a.identity.display_name.clone(),
            email: a.identity.email.to_string(),
            signature_html: a.identity.signature_html.clone(),
        })
        .collect()
}

/// The `From` of mail sent with this account.
pub fn sender_address(account: &Account) -> Address {
    let name = account.identity.display_name.trim();
    Address {
        name: (!name.is_empty()).then(|| name.to_owned()),
        email: account.identity.email.clone(),
    }
}

/// Gmail files every SMTP submission into "Sent Mail" itself; appending our
/// own copy there would show the message twice.
pub fn saves_sent_copy_itself(account: &Account) -> bool {
    account.provider_type == mc_domain::ProviderType::GmailApi
        || account.connection_config.imap_host.as_deref().is_some_and(|h| {
            let h = h.to_ascii_lowercase();
            h.ends_with("gmail.com") || h.ends_with("googlemail.com")
        })
}

pub fn new_message_rfc_id(from: &EmailAddress) -> String {
    format!("<{}@{}>", uuid::Uuid::now_v7().simple(), from.domain())
}

// ── Subject ──────────────────────────────────────────────────────────────────

/// Reply/forward markers used by common clients and locales (Italian "R:"/"I:",
/// German "AW:", French "TR:").
const SUBJECT_PREFIXES: &[&str] = &["re", "r", "aw", "fwd", "fw", "i", "tr"];

/// Drop any leading chain of reply/forward markers ("RE: Fwd: R[2]: x" → "x").
pub fn strip_subject_prefixes(subject: &str) -> &str {
    let mut rest = subject.trim_start();
    while let Some(colon) = rest.find(':') {
        let tag = rest[..colon].trim_end();
        // "Re[2]:" counters some clients add.
        let tag = match tag.split_once('[') {
            Some((t, n)) if n.strip_suffix(']').is_some_and(|n| n.chars().all(|c| c.is_ascii_digit())) => {
                t.trim_end()
            }
            _ => tag,
        };
        if !SUBJECT_PREFIXES.iter().any(|p| tag.eq_ignore_ascii_case(p)) {
            break;
        }
        rest = rest[colon + 1..].trim_start();
    }
    rest
}

pub fn prefixed_subject(prefix: &str, subject: &str) -> String {
    format!("{prefix} {}", strip_subject_prefixes(subject))
}

// ── Addresses ────────────────────────────────────────────────────────────────

/// `Name <email>`, or the bare address when there is no useful name.
pub fn format_address(a: &Address) -> String {
    let name = a
        .name
        .as_deref()
        .map(str::trim)
        .filter(|n| !n.is_empty() && !n.eq_ignore_ascii_case(a.email.as_str()));
    match name {
        Some(n) if n.contains([',', ';', '<', '>', '"', '@', ':']) => {
            format!("\"{}\" <{}>", n.replace('"', ""), a.email)
        }
        Some(n) => format!("{n} <{}>", a.email),
        None => a.email.to_string(),
    }
}

/// Parse one recipient as typed or picked in the UI.
pub fn parse_address(raw: &str) -> Result<Address, ApplicationError> {
    let invalid = || ApplicationError::PreconditionFailed(format!("Indirizzo non valido: «{}»", raw.trim()));
    let list = mailparse::addrparse(raw.trim()).map_err(|_| invalid())?;
    let [MailAddr::Single(info)] = list.as_slice() else {
        return Err(invalid());
    };
    let email = EmailAddress::new(&info.addr).map_err(|_| invalid())?;
    Ok(Address {
        name: info.display_name.clone().filter(|n| !n.trim().is_empty()),
        email,
    })
}

pub fn parse_addresses(raw: &[String]) -> Result<Vec<Address>, ApplicationError> {
    raw.iter()
        .filter(|s| !s.trim().is_empty())
        .map(|s| parse_address(s))
        .collect()
}

fn format_addresses(list: &[Address]) -> Vec<String> {
    list.iter().map(format_address).collect()
}

/// The account a reply is sent from: the one the original was addressed to,
/// else the account the message lives in.
pub fn reply_sender(accounts: &[Account], original: &Envelope, fallback: AccountId) -> AccountId {
    original
        .to
        .iter()
        .chain(&original.cc)
        .find_map(|a| accounts.iter().find(|acc| acc.identity.email == a.email))
        .map_or(fallback, |acc| acc.id)
}

/// (To, Cc) of a reply. `own` are all configured account addresses.
pub fn reply_recipients(original: &Envelope, own: &[EmailAddress], all: bool) -> (Vec<String>, Vec<String>) {
    let primary = if original.reply_to.is_empty() { &original.from } else { &original.reply_to };
    // Replying to something we sent goes back to the people we sent it to.
    let primary = if primary.iter().all(|a| own.contains(&a.email)) { &original.to } else { primary };
    if !all {
        return (format_addresses(primary), vec![]);
    }

    let mut seen = HashSet::new();
    let mut keep = |a: &&Address| !own.contains(&a.email) && seen.insert(a.email.clone());
    let to: Vec<Address> = primary.iter().chain(&original.to).filter(&mut keep).cloned().collect();
    let cc: Vec<Address> = original.cc.iter().filter(&mut keep).cloned().collect();
    (format_addresses(&to), format_addresses(&cc))
}

// ── Quoting ──────────────────────────────────────────────────────────────────

const MONTHS: [&str; 12] = ["gen", "feb", "mar", "apr", "mag", "giu", "lug", "ago", "set", "ott", "nov", "dic"];

/// "12 ott 2026, 09:05"
pub fn format_date<Tz: TimeZone>(date: &DateTime<Tz>) -> String
where
    Tz::Offset: std::fmt::Display,
{
    format!(
        "{} {} {}, {}",
        date.format("%d"),
        MONTHS[date.month0() as usize],
        date.year(),
        date.format("%H:%M")
    )
}

/// The original message rendered below the user's text, in both body flavours.
#[derive(Debug, Clone)]
pub struct Quote {
    pub html: String,
    pub text: String,
}

pub fn reply_quote<Tz: TimeZone>(original: &Envelope, date: &DateTime<Tz>, body_text: &str) -> Quote
where
    Tz::Offset: std::fmt::Display,
{
    let sender = original.from.first().map(format_address).unwrap_or_default();
    let attribution = format!("Il {}, {sender} ha scritto:", format_date(date));
    Quote {
        html: format!(
            "<div>{}</div><blockquote type=\"cite\"><div style=\"white-space: pre-wrap\">{}</div></blockquote>",
            escape_html(&attribution),
            escape_html(body_text.trim_end())
        ),
        text: format!("{attribution}\n{}", quote_lines(body_text)),
    }
}

pub fn forward_quote<Tz: TimeZone>(
    original: &Envelope,
    date: &DateTime<Tz>,
    body_html: Option<&str>,
    body_text: &str,
) -> Quote
where
    Tz::Offset: std::fmt::Display,
{
    let join = |list: &[Address]| format_addresses(list).join(", ");
    let mut fields = vec![
        ("Da", join(&original.from)),
        ("Oggetto", original.subject.clone()),
        ("Data", format_date(date)),
        ("A", join(&original.to)),
    ];
    if !original.cc.is_empty() {
        fields.push(("Cc", join(&original.cc)));
    }
    let header_html: String = fields
        .iter()
        .map(|(k, v)| format!("<div><b>{k}:</b> {}</div>", escape_html(v)))
        .collect();
    let header_text: String = fields.iter().map(|(k, v)| format!("{k}: {v}\n")).collect();
    let content = match body_html {
        Some(html) => body_inner_html(html),
        None => format!("<pre>{}</pre>", escape_html(body_text)),
    };
    Quote {
        html: format!(
            "<div>Inizio messaggio inoltrato:</div><blockquote type=\"cite\">{header_html}<br>{content}</blockquote>"
        ),
        text: format!("Inizio messaggio inoltrato:\n\n{header_text}\n{}", quote_lines(body_text)),
    }
}

/// The quote a reply or forward of `source` carries; `None` for other modes.
pub fn quote_for(mode: ComposeMode, source: &mc_domain::Message) -> Option<Quote> {
    let text = source
        .body_text
        .clone()
        .or_else(|| source.body_html.as_deref().map(html_to_text))
        .unwrap_or_default();
    let date = source.envelope.date.with_timezone(&chrono::Local);
    match mode {
        ComposeMode::Reply | ComposeMode::ReplyAll => Some(reply_quote(&source.envelope, &date, &text)),
        ComposeMode::Forward => Some(forward_quote(&source.envelope, &date, source.body_html.as_deref(), &text)),
        ComposeMode::New | ComposeMode::Draft => None,
    }
}

/// Prefix every line with "> " (">" for already-quoted or empty lines, as
/// mail clients nest quotes).
pub fn quote_lines(text: &str) -> String {
    text.trim_end()
        .lines()
        .map(|l| match l {
            "" => ">".to_owned(),
            l if l.starts_with('>') => format!(">{l}"),
            l => format!("> {l}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

/// The content of `<body>` without `<script>` elements, so a full HTML
/// document can be nested inside a blockquote.
pub fn body_inner_html(html: &str) -> String {
    let lower = html.to_ascii_lowercase();
    let start = lower
        .find("<body")
        .and_then(|i| lower[i..].find('>').map(|j| i + j + 1))
        .unwrap_or(0);
    let end = lower.rfind("</body>").filter(|&e| e >= start).unwrap_or(html.len());
    strip_elements(&html[start..end], &["script"])
}

fn strip_elements(html: &str, tags: &[&str]) -> String {
    let mut out = html.to_owned();
    for tag in tags {
        let (open, close) = (format!("<{tag}"), format!("</{tag}>"));
        while let Some(start) = out.to_ascii_lowercase().find(&open) {
            let lower = out.to_ascii_lowercase();
            let end = lower[start..].find(&close).map_or(out.len(), |e| start + e + close.len());
            out.replace_range(start..end, "");
        }
    }
    out
}

/// Readable text from an HTML body, for the text/plain quote of HTML-only mail.
/// ponytail: tag stripping, not rendering — tables and lists come out flat.
pub fn html_to_text(html: &str) -> String {
    let html = strip_elements(&body_inner_html(html), &["style", "head"]);
    let mut text = String::with_capacity(html.len());
    let mut rest = html.as_str();
    while let Some(lt) = rest.find('<') {
        text.push_str(&rest[..lt]);
        let Some(gt) = rest[lt..].find('>') else {
            rest = "";
            break;
        };
        let tag = rest[lt + 1..lt + gt].trim_start_matches('/').to_ascii_lowercase();
        let name = tag.split(|c: char| c.is_whitespace() || c == '/').next().unwrap_or("");
        if matches!(name, "br" | "p" | "div" | "tr" | "li" | "h1" | "h2" | "h3" | "h4" | "h5" | "h6" | "blockquote") {
            text.push('\n');
        }
        rest = &rest[lt + gt + 1..];
    }
    text.push_str(rest);

    let text = text
        .replace("&nbsp;", " ")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&amp;", "&");
    let mut out = String::new();
    let mut blank_run = 0;
    for line in text.lines().map(str::trim_end) {
        blank_run = if line.trim().is_empty() { blank_run + 1 } else { 0 };
        if blank_run <= 1 {
            out.push_str(line);
            out.push('\n');
        }
    }
    out.trim().to_owned()
}

// ── Bodies ───────────────────────────────────────────────────────────────────

/// GFM-flavoured markdown to HTML. Single newlines become `<br>`: people type
/// mail line by line and expect Enter to start a new line.
pub fn markdown_to_html(md: &str) -> String {
    let options = Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS;
    let events = Parser::new_ext(md, options).map(|e| match e {
        Event::SoftBreak => Event::HardBreak,
        e => e,
    });
    let mut html = String::with_capacity(md.len() * 3 / 2);
    pulldown_cmark::html::push_html(&mut html, events);
    html
}

pub fn assemble_html(body_md: &str, signature_html: Option<&str>, quote: Option<&Quote>) -> String {
    let mut body = format!("<div>{}</div>", markdown_to_html(body_md));
    if let Some(sig) = signature_html.filter(|s| !s.trim().is_empty()) {
        body.push_str(&format!("<br><div class=\"signature\">{sig}</div>"));
    }
    if let Some(q) = quote {
        body.push_str("<br>");
        body.push_str(&q.html);
    }
    format!("<!DOCTYPE html><html><head><meta charset=\"utf-8\"></head><body>{body}</body></html>")
}

pub fn assemble_text(body_md: &str, signature_text: Option<&str>, quote: Option<&Quote>) -> String {
    let mut text = body_md.trim_end().to_owned();
    if let Some(sig) = signature_text.filter(|s| !s.trim().is_empty()) {
        text.push_str("\n\n-- \n");
        text.push_str(sig.trim_end());
    }
    if let Some(q) = quote {
        text.push_str("\n\n");
        text.push_str(&q.text);
    }
    text
}

pub fn signature_text(account: &Account) -> Option<String> {
    let identity = &account.identity;
    identity
        .signature_text
        .clone()
        .or_else(|| identity.signature_html.as_deref().map(html_to_text))
}

// ── Existing messages (forward source, stored drafts) ────────────────────────

const MARKDOWN_HEADER: &str = "X-Mail-Cat-Markdown";
const MODE_HEADER: &str = "X-Mail-Cat-Mode";
const SOURCE_HEADER: &str = "X-Mail-Cat-Source";

/// What a stored draft remembers beyond plain MIME.
#[derive(Debug, Clone, PartialEq)]
pub struct DraftMeta {
    pub mode: ComposeMode,
    pub source_message_id: Option<MessageId>,
    /// `None` for drafts written by other clients.
    pub body_md: Option<String>,
}

pub fn draft_headers(mode: ComposeMode, source: Option<MessageId>, body_md: &str) -> Vec<(String, String)> {
    // Split into words so the header encoder can fold the base64 under the
    // 998-octet line limit; whitespace is dropped again when decoding.
    let encoded = BASE64.encode(body_md);
    let folded = encoded
        .as_bytes()
        .chunks(76)
        .map(|c| std::str::from_utf8(c).expect("base64 is ASCII"))
        .collect::<Vec<_>>()
        .join(" ");
    let mut headers = vec![
        (MODE_HEADER.to_owned(), mode.as_str().to_owned()),
        (MARKDOWN_HEADER.to_owned(), folded),
    ];
    if let Some(id) = source {
        headers.push((SOURCE_HEADER.to_owned(), id.to_string()));
    }
    headers
}

fn draft_meta(headers: &[MailHeader]) -> DraftMeta {
    let body_md = headers.get_first_value(MARKDOWN_HEADER).and_then(|v| {
        let compact: String = v.split_whitespace().collect();
        BASE64.decode(compact).ok().and_then(|b| String::from_utf8(b).ok())
    });
    DraftMeta {
        mode: headers
            .get_first_value(MODE_HEADER)
            .and_then(|m| ComposeMode::parse(m.trim()))
            .unwrap_or(ComposeMode::New),
        source_message_id: headers
            .get_first_value(SOURCE_HEADER)
            .and_then(|v| uuid::Uuid::parse_str(v.trim()).ok())
            .map(MessageId::from_uuid),
        body_md,
    }
}

/// An existing message taken apart: bodies, non-body parts and the headers a
/// draft restore needs.
#[derive(Debug, Clone, Default)]
pub struct ParsedSource {
    pub text: Option<String>,
    pub html: Option<String>,
    /// Attachments and inline images, in MIME order.
    pub parts: Vec<OutgoingAttachment>,
    pub subject: String,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub draft: Option<DraftMeta>,
}

pub fn parse_source(raw: &[u8]) -> Result<ParsedSource, ApplicationError> {
    let mail = mailparse::parse_mail(raw)
        .map_err(|e| ApplicationError::Internal(format!("messaggio originale illeggibile: {e}")))?;
    let addresses = |key: &str| -> Vec<String> {
        let Some(header) = mail.headers.get_first_header(key) else {
            return vec![];
        };
        let Ok(list) = mailparse::addrparse_header(header) else {
            return vec![header.get_value()];
        };
        list.iter()
            .flat_map(|a| match a {
                MailAddr::Single(s) => vec![s.clone()],
                MailAddr::Group(g) => g.addrs.clone(),
            })
            .map(|s| match EmailAddress::new(&s.addr) {
                Ok(email) => format_address(&Address { name: s.display_name, email }),
                Err(_) => s.addr,
            })
            .collect()
    };
    let mut parsed = ParsedSource {
        subject: mail.headers.get_first_value("Subject").unwrap_or_default(),
        to: addresses("To"),
        cc: addresses("Cc"),
        bcc: addresses("Bcc"),
        draft: mail
            .headers
            .get_first_header(MODE_HEADER)
            .map(|_| draft_meta(&mail.headers)),
        ..Default::default()
    };
    collect_parts(&mail, &mut parsed);
    Ok(parsed)
}

fn collect_parts(part: &ParsedMail, out: &mut ParsedSource) {
    if !part.subparts.is_empty() {
        for sub in &part.subparts {
            collect_parts(sub, out);
        }
        return;
    }
    let mime = part.ctype.mimetype.to_ascii_lowercase();
    let disposition = part.get_content_disposition();
    let filename = disposition
        .params
        .get("filename")
        .or_else(|| part.ctype.params.get("name"))
        .cloned();
    let is_attachment = disposition.disposition == DispositionType::Attachment || filename.is_some();
    if !is_attachment {
        if mime == "text/plain" && out.text.is_none() {
            out.text = part.get_body().ok();
            return;
        }
        if mime == "text/html" && out.html.is_none() {
            out.html = part.get_body().ok();
            return;
        }
    }
    let content_id = part
        .headers
        .get_first_value("Content-ID")
        .map(|v| v.trim().trim_start_matches('<').trim_end_matches('>').to_owned());
    if !is_attachment && content_id.is_none() {
        return;
    }
    let Ok(data) = part.get_body_raw() else { return };
    out.parts.push(OutgoingAttachment {
        filename: filename.unwrap_or_else(|| "allegato".to_owned()),
        mime_type: mime,
        content_id,
        data,
    });
}

/// Is this part shown inline by `html` (so it travels with it, unlisted)?
pub fn referenced_inline(part: &OutgoingAttachment, html: &str) -> bool {
    part.content_id.as_ref().is_some_and(|cid| html.contains(&format!("cid:{cid}")))
}

/// Content fingerprint of a part. Attachment ids carry it instead of a MIME
/// position because saving a draft replaces the stored message the id was
/// minted from: the kept part is then found again, byte-identical, in the new
/// version. Only needs to be stable for the lifetime of a compose window.
pub fn part_fingerprint(part: &OutgoingAttachment) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    (&part.filename, &part.mime_type, &part.data).hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// The parts the user sees and may drop: everything not shown inline.
pub fn listable_parts(message_id: MessageId, parsed: &ParsedSource, html: &str) -> Vec<ForwardedAttachment> {
    parsed
        .parts
        .iter()
        .filter(|p| !referenced_inline(p, html))
        .map(|p| ForwardedAttachment {
            id: format!("{message_id}:{}", part_fingerprint(p)),
            filename: p.filename.clone(),
            size: p.data.len() as u64,
            mime_type: p.mime_type.clone(),
        })
        .collect()
}

/// `<message id>:<fingerprint>` → its parts.
pub fn parse_attachment_id(id: &str) -> Result<(MessageId, String), ApplicationError> {
    let invalid = || ApplicationError::PreconditionFailed(format!("allegato sconosciuto: {id}"));
    let (message, fingerprint) = id.split_once(':').ok_or_else(invalid)?;
    let message = uuid::Uuid::parse_str(message).map_err(|_| invalid())?;
    Ok((MessageId::from_uuid(message), fingerprint.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use mc_domain::{AuthConfig, ConnectionConfig, Identity, ProviderType};

    fn addr(name: Option<&str>, email: &str) -> Address {
        Address {
            name: name.map(Into::into),
            email: EmailAddress::new(email).unwrap(),
        }
    }

    fn account(email: &str) -> Account {
        let email = EmailAddress::new(email).unwrap();
        Account::create(
            "Test".into(),
            email.clone(),
            ProviderType::Imap,
            ConnectionConfig {
                imap_host: Some("imap.example.com".into()),
                imap_port: Some(993),
                use_tls: true,
                auth: AuthConfig::Plain { username: "u".into(), password: String::new() },
                smtp: None,
                smtp_host: None,
                smtp_port: None,
            },
            Identity { display_name: "Me".into(), email, signature_html: None, signature_text: None },
        )
        .0
    }

    fn envelope() -> Envelope {
        Envelope {
            from: vec![addr(Some("Anna Bianchi"), "anna@example.com")],
            to: vec![addr(None, "me@example.com"), addr(Some("Bob"), "bob@example.com")],
            cc: vec![addr(None, "carl@example.com"), addr(None, "ME@example.com")],
            bcc: vec![],
            reply_to: vec![],
            subject: "Budget".into(),
            date: Utc::now(),
        }
    }

    #[test]
    fn subject_prefix_chains_collapse_to_one() {
        assert_eq!(prefixed_subject("Re:", "Budget"), "Re: Budget");
        assert_eq!(prefixed_subject("Re:", "RE: R: Re[2]: I: Fwd: FW: aw: TR: Budget"), "Re: Budget");
        assert_eq!(prefixed_subject("Fwd:", "Re : Budget"), "Fwd: Budget");
        assert_eq!(prefixed_subject("Re:", "Ordine: 12"), "Re: Ordine: 12");
        assert_eq!(prefixed_subject("Re:", "Ire: fumo"), "Re: Ire: fumo");
        assert_eq!(prefixed_subject("Re:", ""), "Re: ");
    }

    #[test]
    fn reply_goes_to_reply_to_when_present() {
        let mut env = envelope();
        assert_eq!(reply_recipients(&env, &[], false).0, ["Anna Bianchi <anna@example.com>"]);
        env.reply_to = vec![addr(None, "list@example.com")];
        let (to, cc) = reply_recipients(&env, &[], false);
        assert_eq!(to, ["list@example.com"]);
        assert!(cc.is_empty());
    }

    #[test]
    fn reply_to_own_message_goes_to_its_recipients() {
        let mut env = envelope();
        env.from = vec![addr(None, "me@example.com")];
        let own = [EmailAddress::new("me@example.com").unwrap()];
        assert_eq!(reply_recipients(&env, &own, false).0, ["me@example.com", "Bob <bob@example.com>"]);
    }

    #[test]
    fn reply_all_keeps_cc_drops_own_addresses_and_duplicates() {
        let mut env = envelope();
        env.to.push(addr(Some("Anna again"), "Anna@Example.com"));
        let own = [EmailAddress::new("me@example.com").unwrap()];
        let (to, cc) = reply_recipients(&env, &own, true);
        assert_eq!(to, ["Anna Bianchi <anna@example.com>", "Bob <bob@example.com>"]);
        assert_eq!(cc, ["carl@example.com"]);
    }

    #[test]
    fn reply_all_does_not_drop_addresses_that_merely_contain_ours() {
        let mut env = envelope();
        env.cc = vec![addr(None, "notme@example.com")];
        let own = [EmailAddress::new("me@example.com").unwrap()];
        assert_eq!(reply_recipients(&env, &own, true).1, ["notme@example.com"]);
    }

    #[test]
    fn reply_sender_is_the_addressed_account() {
        let (work, home) = (account("work@example.com"), account("me@example.com"));
        let accounts = [work.clone(), home.clone()];
        let fallback = AccountId::new();
        assert_eq!(reply_sender(&accounts, &envelope(), fallback), home.id);

        let mut env = envelope();
        env.to = vec![addr(None, "someone@example.com")];
        env.cc = vec![addr(None, "WORK@example.com")];
        assert_eq!(reply_sender(&accounts, &env, fallback), work.id);

        env.cc.clear();
        assert_eq!(reply_sender(&accounts, &env, fallback), fallback);
    }

    #[test]
    fn addresses_round_trip_through_ui_strings() {
        let parsed = parse_address("\"Rossi, Mario\" <Mario@Example.com>").unwrap();
        assert_eq!(parsed.name.as_deref(), Some("Rossi, Mario"));
        assert_eq!(format_address(&parsed), "\"Rossi, Mario\" <mario@example.com>");
        assert_eq!(format_address(&parse_address("bob@example.com").unwrap()), "bob@example.com");

        let err = parse_address("not an address").unwrap_err().to_string();
        assert!(err.contains("not an address"), "{err}");
        assert!(parse_addresses(&["a@example.com".into(), " ".into()]).unwrap().len() == 1);
    }

    #[test]
    fn plain_text_quote_prefixes_every_line() {
        assert_eq!(quote_lines("ciao\n\n> prima\nfine\n"), "> ciao\n>\n>> prima\n> fine");
        let date = Utc.with_ymd_and_hms(2026, 10, 2, 9, 5, 0).unwrap();
        let quote = reply_quote(&envelope(), &date, "riga 1\nriga <2>");
        assert_eq!(
            quote.text,
            "Il 02 ott 2026, 09:05, Anna Bianchi <anna@example.com> ha scritto:\n> riga 1\n> riga <2>"
        );
        assert!(quote.html.contains("<blockquote type=\"cite\">"));
        assert!(quote.html.contains("riga &lt;2&gt;"), "{}", quote.html);
    }

    #[test]
    fn forward_quote_has_header_block_and_original_html() {
        let date = Utc.with_ymd_and_hms(2026, 1, 15, 18, 30, 0).unwrap();
        let html = "<html><head><style>p{}</style></head><body><p>Ciao</p><script>x()</script></body></html>";
        let quote = forward_quote(&envelope(), &date, Some(html), "Ciao");
        assert!(quote.html.starts_with("<div>Inizio messaggio inoltrato:</div>"));
        assert!(quote.html.contains("<b>Oggetto:</b> Budget"));
        assert!(quote.html.contains("<b>Data:</b> 15 gen 2026, 18:30"));
        assert!(quote.html.contains("<b>Cc:</b> carl@example.com, me@example.com"));
        assert!(quote.html.contains("<p>Ciao</p>") && !quote.html.contains("script") && !quote.html.contains("<style>"));
        assert!(quote.text.contains("Da: Anna Bianchi <anna@example.com>\n"));
        assert!(quote.text.ends_with("> Ciao"));
    }

    #[test]
    fn bodies_put_text_then_signature_then_quote() {
        let quote = Quote { html: "<blockquote>Q</blockquote>".into(), text: "> Q".into() };
        let html = assemble_html("**ciao**\nriga\n\n| a |\n|---|\n| 1 |\n\n~~no~~\n\n- [x] ok", Some("<i>Mario</i>"), Some(&quote));
        let (text_at, sig_at, quote_at) = (
            html.find("<strong>ciao</strong><br />").unwrap(),
            html.find("<i>Mario</i>").unwrap(),
            html.find("<blockquote>Q").unwrap(),
        );
        assert!(text_at < sig_at && sig_at < quote_at);
        assert!(html.contains("<table>") && html.contains("<del>no</del>") && html.contains("checkbox"));

        assert_eq!(assemble_text("ciao\n", Some("Mario"), Some(&quote)), "ciao\n\n-- \nMario\n\n> Q");
        assert_eq!(assemble_text("ciao", None, None), "ciao");
    }

    #[test]
    fn html_to_text_keeps_line_structure() {
        assert_eq!(
            html_to_text("<html><body><p>Uno &amp; due</p><div>tre<br>quattro</div></body></html>"),
            "Uno & due\n\ntre\nquattro"
        );
    }

    #[test]
    fn draft_metadata_survives_a_mime_round_trip() {
        let source = MessageId::new();
        let md = "# Titolo\n\nàèìòù ".repeat(200);
        let headers: String = draft_headers(ComposeMode::Reply, Some(source), &md)
            .into_iter()
            // Shape of what the header encoder writes: folded at the spaces.
            .map(|(k, v)| format!("{k}: {}\r\n", v.replace(' ', "\r\n ")))
            .collect();
        let raw = format!("{headers}To: \"Rossi, Mario\" <m@example.com>, b@example.com\r\nSubject: Re: x\r\n\r\nbody");
        let parsed = parse_source(raw.as_bytes()).unwrap();
        assert_eq!(
            parsed.draft,
            Some(DraftMeta { mode: ComposeMode::Reply, source_message_id: Some(source), body_md: Some(md) })
        );
        assert_eq!(parsed.to, ["\"Rossi, Mario\" <m@example.com>", "b@example.com"]);
        assert_eq!(parsed.text.as_deref(), Some("body"));
    }

    #[test]
    fn source_parts_split_bodies_from_attachments_and_inline_images() {
        let raw = "Content-Type: multipart/mixed; boundary=\"M\"\r\n\r\n\
--M\r\nContent-Type: multipart/related; boundary=\"R\"\r\n\r\n\
--R\r\nContent-Type: text/html\r\n\r\n<p><img src=\"cid:logo@x\"></p>\r\n\
--R\r\nContent-Type: image/png\r\nContent-ID: <logo@x>\r\nContent-Transfer-Encoding: base64\r\n\r\nAQID\r\n\
--R--\r\n\
--M\r\nContent-Type: application/pdf; name=\"a.pdf\"\r\nContent-Disposition: attachment; filename=\"a.pdf\"\r\nContent-Transfer-Encoding: base64\r\n\r\nBAUG\r\n\
--M--\r\n";
        let parsed = parse_source(raw.as_bytes()).unwrap();
        assert!(parsed.html.as_deref().unwrap().contains("cid:logo@x"));
        assert_eq!(parsed.parts.len(), 2);
        assert_eq!(parsed.parts[0].content_id.as_deref(), Some("logo@x"));
        assert_eq!(parsed.parts[0].data, [1, 2, 3]);

        let id = MessageId::new();
        let listed = listable_parts(id, &parsed, parsed.html.as_deref().unwrap());
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].filename, "a.pdf");
        assert_eq!(parse_attachment_id(&listed[0].id).unwrap(), (id, part_fingerprint(&parsed.parts[1])));
        assert_ne!(part_fingerprint(&parsed.parts[0]), part_fingerprint(&parsed.parts[1]));
    }
}
