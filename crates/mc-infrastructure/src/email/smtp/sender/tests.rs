use super::*;
use std::sync::Mutex;

#[test]
fn only_535_rejects_credentials() {
    let code = |severity, category, detail| Code::new(severity, category, detail);
    use Severity::{
        PermanentNegativeCompletion as Permanent, TransientNegativeCompletion as Transient,
    };
    assert!(rejects_credentials(code(
        Permanent,
        Category::Unspecified3,
        Detail::Five
    )));
    assert!(
        !rejects_credentials(code(Permanent, Category::Unspecified3, Detail::Zero)),
        "530 asks to log in first"
    );
    assert!(
        !rejects_credentials(code(Permanent, Category::MailSystem, Detail::Zero)),
        "550 refused recipient"
    );
    assert!(!rejects_credentials(code(
        Transient,
        Category::Unspecified3,
        Detail::Four
    )));
}

#[test]
fn a_challenged_xoauth2_login_is_a_refused_token() {
    // What lettre fails with when the server answers XOAUTH2 with its 334 error challenge.
    let creds = Credentials::new("me@gmail.com".into(), "ya29.stale".into());
    let e = Mechanism::Xoauth2
        .response(&creds, Some("eyJzdGF0dXMiOiI0MDAifQ=="))
        .unwrap_err();
    assert!(matches!(
        smtp_error("SMTP send error", &e, true),
        DomainError::AuthenticationFailed(_)
    ));
    assert!(
        !smtp_error("SMTP send error", &e, true)
            .to_string()
            .contains("ya29"),
        "never echoes the token"
    );
    // The same client error on a PLAIN login is not a refused token.
    assert!(!matches!(
        smtp_error("SMTP send error", &e, false),
        DomainError::AuthenticationFailed(_)
    ));
}

/// Records what SMTP tells the account's gate.
#[derive(Default)]
struct Recorder {
    secret: Mutex<Option<LoginSecret>>,
    judged: Mutex<Vec<bool>>,
    smtp: Mutex<Vec<Option<String>>>,
}

#[allow(clippy::double_must_use)]
#[async_trait::async_trait]
impl LoginGate for Recorder {
    async fn secret(&self) -> Result<LoginSecret, DomainError> {
        Ok(self.secret.lock().unwrap().clone().unwrap())
    }
    async fn login_failed(&self, _: &LoginSecret, _: &DomainError, _: bool) -> bool {
        panic!("SMTP must not judge the account");
    }
    async fn smtp_token_refused(&self, rejected: &LoginSecret) -> bool {
        self.judged
            .lock()
            .unwrap()
            .push(matches!(rejected, LoginSecret::OAuthToken(_)));
        matches!(rejected, LoginSecret::OAuthToken(_))
    }
    fn login_succeeded(&self) {
        panic!("SMTP must not report a login to the account state");
    }
    async fn ready(&self) {}
    fn smtp_login(&self, refused: Option<String>) {
        self.smtp.lock().unwrap().push(refused);
    }
}

fn refused() -> DomainError {
    DomainError::AuthenticationFailed(
        "permanent error (535): 5.7.8 Username and Password not accepted".into(),
    )
}

/// A wrong `[accounts.smtp] username`: the send fails, the user is told,
/// and the account's state is never asked to change.
#[tokio::test]
async fn a_refused_smtp_password_never_judges_the_account() {
    let gate = Recorder::default();
    *gate.secret.lock().unwrap() = Some(LoginSecret::Password("hunter2".into()));

    let result: Result<(), _> = login_with(&SmtpView(&gate), |_| async { Err(refused()) }).await;

    assert!(matches!(result, Err(DomainError::AuthenticationFailed(_))));
    assert_eq!(
        *gate.judged.lock().unwrap(),
        [false],
        "no refresh for a password, no verdict"
    );
    assert_eq!(gate.smtp.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_refused_smtp_token_is_refreshed_once_and_never_pauses() {
    let gate = Recorder::default();
    *gate.secret.lock().unwrap() = Some(LoginSecret::OAuthToken("ya29.a".into()));
    let attempts = Mutex::new(0);

    let result: Result<(), _> = login_with(&SmtpView(&gate), |_| {
        *attempts.lock().unwrap() += 1;
        async { Err(refused()) }
    })
    .await;

    assert!(result.is_err());
    assert_eq!(
        *attempts.lock().unwrap(),
        2,
        "one retry after the forced refresh"
    );
    assert_eq!(
        *gate.judged.lock().unwrap(),
        [true],
        "one refresh request, nothing more"
    );
}

fn addr(email: &str) -> Address {
    Address {
        name: None,
        email: mc_domain::EmailAddress::new(email).unwrap(),
    }
}

fn outgoing() -> OutgoingMessage {
    OutgoingMessage {
        from: Address {
            name: Some("Mario Rossi".into()),
            email: mc_domain::EmailAddress::new("mario@example.com").unwrap(),
        },
        to: vec![addr("anna@example.com")],
        cc: vec![],
        bcc: vec![addr("hidden@example.com")],
        subject: "Ciao".into(),
        body_text: Some("testo".into()),
        body_html: Some("<p>testo <img src=\"cid:img1\"></p>".into()),
        in_reply_to: Some("<orig@example.com>".into()),
        references: vec!["<root@example.com>".into(), "<orig@example.com>".into()],
        attachments: vec![],
        message_rfc_id: Some("<new@example.com>".into()),
        extra_headers: vec![("X-Mail-Cat-Mode".into(), "reply".into())],
    }
}

fn attachment(name: &str, cid: Option<&str>) -> OutgoingAttachment {
    OutgoingAttachment {
        filename: name.into(),
        mime_type: "image/png".into(),
        content_id: cid.map(Into::into),
        data: vec![1, 2, 3],
    }
}

fn formatted(msg: &OutgoingMessage, keep_bcc: bool) -> String {
    String::from_utf8(build_message(msg, keep_bcc).unwrap().formatted()).unwrap()
}

#[test]
fn mime_nests_mixed_related_alternative() {
    let mut msg = outgoing();
    msg.attachments = vec![
        attachment("doc.png", None),
        attachment("img1.png", Some("img1")),
    ];
    let raw = formatted(&msg, false);

    let mixed = raw.find("multipart/mixed").expect("mixed layer");
    let related = raw.find("multipart/related").expect("related layer");
    let alternative = raw
        .find("multipart/alternative")
        .expect("alternative layer");
    assert!(mixed < related && related < alternative, "{raw}");
    assert!(raw.contains("Content-ID: <img1>"), "{raw}");
    assert!(
        raw.contains("Content-Disposition: attachment; filename=\"doc.png\""),
        "{raw}"
    );
    assert!(
        raw.contains("From: \"Mario Rossi\" <mario@example.com>")
            || raw.contains("From: Mario Rossi <mario@example.com>"),
        "{raw}"
    );
    assert!(raw.contains("Message-ID: <new@example.com>"), "{raw}");
    assert!(raw.contains("In-Reply-To: <orig@example.com>"), "{raw}");
    assert!(raw.contains("X-Mail-Cat-Mode: reply"), "{raw}");
}

#[test]
fn mime_omits_empty_layers() {
    let raw = formatted(&outgoing(), false);
    assert!(raw.contains("multipart/alternative"));
    assert!(
        !raw.contains("multipart/mixed") && !raw.contains("multipart/related"),
        "{raw}"
    );

    let mut plain = outgoing();
    plain.body_html = None;
    plain.attachments = vec![attachment("a.png", Some("ignored-without-html"))];
    let raw = formatted(&plain, false);
    assert!(
        raw.contains("multipart/mixed") && !raw.contains("multipart/alternative"),
        "{raw}"
    );
}

#[test]
fn long_extra_header_is_folded_and_unfolds_intact() {
    use mailparse::MailHeaderMap;
    let words: Vec<String> = (0..400).map(|i| format!("{i:076}")).collect();
    let mut msg = outgoing();
    msg.extra_headers = vec![("X-Mail-Cat-Markdown".into(), words.join(" "))];
    let raw = format_for_storage(&msg).unwrap();
    assert!(raw.split(|b| *b == b'\n').all(|l| l.len() <= 998));
    let parsed = mailparse::parse_mail(&raw).unwrap();
    let value = parsed
        .headers
        .get_first_value("X-Mail-Cat-Markdown")
        .unwrap();
    assert_eq!(value.split_whitespace().collect::<Vec<_>>(), words);
}

#[test]
fn bcc_is_kept_only_in_the_stored_copy() {
    assert!(!formatted(&outgoing(), false).contains("hidden@example.com"));
    assert!(formatted(&outgoing(), true).contains("Bcc: hidden@example.com"));
}
