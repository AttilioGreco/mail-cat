use super::super::attachments::attachments;
use super::*;

fn named(response: &[u8]) -> Vec<(Option<String>, String, bool)> {
    use async_imap::imap_proto::{AttributeValue, Response};
    let (_, Response::Fetch(_, attrs)) = Response::parse(response).unwrap() else {
        panic!("not a FETCH response")
    };
    let structure = attrs
        .iter()
        .find_map(|a| match a {
            AttributeValue::BodyStructure(s) => Some(s),
            _ => None,
        })
        .unwrap();
    attachments(structure)
        .into_iter()
        .map(|a| (a.filename, a.mime_type, a.is_inline))
        .collect()
}

#[test]
fn nested_parts_are_named_and_the_text_body_is_not() {
    // mixed( related( alternative(plain, html), inline logo ), pdf, forwarded message )
    let found = named(
        b"* 1 FETCH (UID 7 BODYSTRUCTURE ((((\"TEXT\" \"PLAIN\" (\"CHARSET\" \"utf-8\") NIL NIL \"7BIT\" 12 1 NIL NIL NIL)\
(\"TEXT\" \"HTML\" (\"CHARSET\" \"utf-8\") NIL NIL \"7BIT\" 40 1 NIL NIL NIL) \"ALTERNATIVE\" (\"BOUNDARY\" \"b3\") NIL NIL)\
(\"IMAGE\" \"PNG\" (\"NAME\" \"logo.png\") \"<cid1>\" NIL \"BASE64\" 4000 NIL (\"INLINE\" (\"FILENAME\" \"logo.png\")) NIL) \"RELATED\" (\"BOUNDARY\" \"b2\") NIL NIL)\
(\"APPLICATION\" \"PDF\" (\"NAME\" \"report.pdf\") NIL NIL \"BASE64\" 9000 NIL (\"ATTACHMENT\" (\"FILENAME\" \"report.pdf\")) NIL)\
(\"MESSAGE\" \"RFC822\" NIL NIL NIL \"7BIT\" 500 (NIL \"fwd\" NIL NIL NIL NIL NIL NIL NIL NIL) (\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 3 1 NIL NIL NIL) 20 NIL (\"ATTACHMENT\" (\"FILENAME\" \"fwd.eml\")) NIL) \"MIXED\" (\"BOUNDARY\" \"b1\") NIL NIL))\r\n",
    );
    assert_eq!(
        found,
        [
            (Some("logo.png".into()), "image/png".into(), true),
            (Some("report.pdf".into()), "application/pdf".into(), false),
            (Some("fwd.eml".into()), "message/rfc822".into(), false),
        ]
    );
}

#[test]
fn encoded_file_names_are_decoded() {
    let found = named(
        b"* 1 FETCH (UID 7 BODYSTRUCTURE ((\"TEXT\" \"PLAIN\" NIL NIL NIL \"7BIT\" 1 1 NIL NIL NIL)\
(\"APPLICATION\" \"PDF\" NIL NIL NIL \"BASE64\" 90 NIL (\"ATTACHMENT\" (\"FILENAME*\" \"utf-8''caff%C3%A8.pdf\")) NIL)\
(\"APPLICATION\" \"PDF\" NIL NIL NIL \"BASE64\" 90 NIL (\"ATTACHMENT\" (\"FILENAME*0*\" \"utf-8''rapporto%20\" \"FILENAME*1*\" \"trimestrale.pdf\")) NIL)\
(\"APPLICATION\" \"MSWORD\" (\"NAME\" \"=?UTF-8?B?Y2l0dMOgLmRvYw==?=\") NIL NIL \"BASE64\" 90 NIL NIL NIL)\
(\"APPLICATION\" \"ZIP\" NIL NIL NIL \"BASE64\" 90 NIL (\"ATTACHMENT\" NIL) NIL) \"MIXED\" (\"BOUNDARY\" \"b\") NIL NIL))\r\n",
    );
    let names: Vec<_> = found.iter().map(|(n, ..)| n.as_deref()).collect();
    assert_eq!(
        names,
        [
            Some("caff\u{e8}.pdf"),
            Some("rapporto trimestrale.pdf"),
            Some("citt\u{e0}.doc"),
            None
        ]
    );
}

#[test]
fn a_plain_message_has_no_attachments() {
    assert!(
        named(b"* 1 FETCH (UID 8 BODYSTRUCTURE (\"TEXT\" \"PLAIN\" (\"CHARSET\" \"utf-8\") NIL NIL \"7BIT\" 3 1 NIL NIL NIL))\r\n")
            .is_empty()
    );
}

#[tokio::test]
async fn the_structure_fetch_names_them_without_downloading_a_part() {
    let (mut session, commands) = scripted(|command| {
        if command.ends_with("(UID BODYSTRUCTURE)") {
            "* 1 FETCH (UID 9 BODYSTRUCTURE ((\"APPLICATION\" \"PDF\" (\"NAME\" \"big.pdf\") NIL NIL \"BASE64\" 9000000 NIL (\"ATTACHMENT\" (\"FILENAME\" \"big.pdf\")) NIL) \"MIXED\" (\"BOUNDARY\" \"b1\") NIL NIL))\r\n$T OK Done\r\n"
        } else {
            "$T OK Done\r\n"
        }
    })
    .await;

    let found = structure_attachments(&mut session, &[9]).await.unwrap();

    assert_eq!(found[&9][0].filename.as_deref(), Some("big.pdf"));
    assert_eq!(found[&9][0].size_bytes, 6_750_000, "decoded size");
    assert_eq!(uid_commands(&commands).len(), 1);
}
