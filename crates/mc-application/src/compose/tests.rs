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
            auth: AuthConfig::Plain {
                username: "u".into(),
                password: String::new(),
            },
            smtp: None,
            smtp_host: None,
            smtp_port: None,
        },
        Identity {
            display_name: "Me".into(),
            email,
            signature_html: None,
            signature_text: None,
        },
    )
    .0
}

fn envelope() -> Envelope {
    Envelope {
        from: vec![addr(Some("Anna Bianchi"), "anna@example.com")],
        to: vec![
            addr(None, "me@example.com"),
            addr(Some("Bob"), "bob@example.com"),
        ],
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
    assert_eq!(
        prefixed_subject("Re:", "RE: R: Re[2]: I: Fwd: FW: aw: TR: Budget"),
        "Re: Budget"
    );
    assert_eq!(prefixed_subject("Fwd:", "Re : Budget"), "Fwd: Budget");
    assert_eq!(prefixed_subject("Re:", "Ordine: 12"), "Re: Ordine: 12");
    assert_eq!(prefixed_subject("Re:", "Ire: fumo"), "Re: Ire: fumo");
    assert_eq!(prefixed_subject("Re:", ""), "Re:");
    assert_eq!(prefixed_subject("Fwd:", " Re: "), "Fwd:");
}

#[test]
fn reply_goes_to_reply_to_when_present() {
    let mut env = envelope();
    assert_eq!(
        reply_recipients(&env, &[], false).0,
        ["Anna Bianchi <anna@example.com>"]
    );
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
    assert_eq!(
        reply_recipients(&env, &own, false).0,
        ["Bob <bob@example.com>"]
    );
}

#[test]
fn reply_to_a_note_to_self_goes_to_self() {
    let mut env = envelope();
    env.from = vec![addr(Some("Me"), "me@example.com")];
    env.to = vec![addr(None, "ME@example.com")];
    env.cc.clear();
    let own = [EmailAddress::new("me@example.com").unwrap()];
    for all in [false, true] {
        assert_eq!(
            reply_recipients(&env, &own, all),
            (vec!["Me <me@example.com>".to_owned()], vec![])
        );
    }
}

#[test]
fn reply_all_keeps_cc_drops_own_addresses_and_duplicates() {
    let mut env = envelope();
    env.to.push(addr(Some("Anna again"), "Anna@Example.com"));
    let own = [EmailAddress::new("me@example.com").unwrap()];
    let (to, cc) = reply_recipients(&env, &own, true);
    assert_eq!(
        to,
        ["Anna Bianchi <anna@example.com>", "Bob <bob@example.com>"]
    );
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
    assert_eq!(
        format_address(&parsed),
        "\"Rossi, Mario\" <mario@example.com>"
    );
    assert_eq!(
        format_address(&parse_address("bob@example.com").unwrap()),
        "bob@example.com"
    );
    for name in ["Smith (Work)", "O\"Neil", "a\\b", "J. Smith"] {
        let formatted = format_address(&addr(Some(name), "x@example.com"));
        assert_eq!(
            parse_address(&formatted).unwrap().name.as_deref(),
            Some(name),
            "{formatted}"
        );
    }
    assert_eq!(
        format_address(&addr(Some("O\"Neil"), "x@example.com")),
        r#""O\"Neil" <x@example.com>"#
    );

    let err = parse_address("not an address").unwrap_err().to_string();
    assert!(err.contains("not an address"), "{err}");
    assert!(
        parse_addresses(&["a@example.com".into(), " ".into()])
            .unwrap()
            .len()
            == 1
    );
}

#[test]
fn plain_text_quote_prefixes_every_line() {
    assert_eq!(
        quote_lines("ciao\n\n> prima\nfine\n"),
        "> ciao\n>\n>> prima\n> fine"
    );
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
    let html =
        "<html><head><style>p{}</style></head><body><p>Ciao</p><script>x()</script></body></html>";
    let quote = forward_quote(&envelope(), &date, Some(html), "Ciao");
    assert!(
        quote
            .html
            .starts_with("<div>Inizio messaggio inoltrato:</div>")
    );
    assert!(quote.html.contains("<b>Oggetto:</b> Budget"));
    assert!(quote.html.contains("<b>Data:</b> 15 gen 2026, 18:30"));
    assert!(
        quote
            .html
            .contains("<b>Cc:</b> carl@example.com, me@example.com")
    );
    assert!(
        quote.html.contains("<p>Ciao</p>")
            && !quote.html.contains("script")
            && !quote.html.contains("<style>")
    );
    assert!(quote.text.contains("Da: Anna Bianchi <anna@example.com>\n"));
    assert!(quote.text.ends_with("> Ciao"));
}

#[test]
fn bodies_put_text_then_signature_then_quote() {
    let quote = Quote {
        html: "<blockquote>Q</blockquote>".into(),
        text: "> Q".into(),
    };
    let html = assemble_html(
        "**ciao**\nriga\n\n| a |\n|---|\n| 1 |\n\n~~no~~\n\n- [x] ok",
        Some("<i>Mario</i>"),
        Some(&quote),
    );
    let (text_at, sig_at, quote_at) = (
        html.find("<strong>ciao</strong><br />").unwrap(),
        html.find("<i>Mario</i>").unwrap(),
        html.find("<blockquote>Q").unwrap(),
    );
    assert!(text_at < sig_at && sig_at < quote_at);
    assert!(
        html.contains("<table>") && html.contains("<del>no</del>") && html.contains("checkbox")
    );

    assert_eq!(
        assemble_text("ciao\n", Some("Mario"), Some(&quote)),
        "ciao\n\n-- \nMario\n\n> Q"
    );
    assert_eq!(assemble_text("ciao", None, None), "ciao");
}

#[test]
fn html_to_text_keeps_line_structure() {
    assert_eq!(
        html_to_text("<html><body><p>Uno &amp; due</p><div>tre<br>quattro</div></body></html>"),
        "Uno & due\n\ntre\nquattro"
    );
    assert_eq!(
        html_to_text("<body><p>Ciao</p><header>Top</header><p>Middle</p><p>End</p></body>"),
        "Ciao\nTop\nMiddle\n\nEnd"
    );
    assert_eq!(
        html_to_text(
            "<STYLE type=x>p{}</Style ><styled-x>A</styled-x><scripts>B</scripts><head/>C"
        ),
        "ABC"
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
    let raw = format!(
        "{headers}To: \"Rossi, Mario\" <m@example.com>, b@example.com\r\nSubject: Re: x\r\n\r\nbody"
    );
    let parsed = parse_source(raw.as_bytes()).unwrap();
    assert_eq!(
        parsed.draft,
        Some(DraftMeta {
            mode: ComposeMode::Reply,
            source_message_id: Some(source),
            body_md: Some(md)
        })
    );
    assert_eq!(
        parsed.to,
        ["\"Rossi, Mario\" <m@example.com>", "b@example.com"]
    );
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
    assert_eq!(
        parse_attachment_id(&listed[0].id).unwrap(),
        (id, part_fingerprint(&parsed.parts[1]))
    );
    assert_ne!(
        part_fingerprint(&parsed.parts[0]),
        part_fingerprint(&parsed.parts[1])
    );
}
