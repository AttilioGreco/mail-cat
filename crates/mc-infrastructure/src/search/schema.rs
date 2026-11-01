//! The index schema and how a message becomes a document.

use tantivy::TantivyDocument;
use tantivy::schema::{
    FAST, Field, INDEXED, IndexRecordOption, STORED, STRING, Schema, TextFieldIndexing, TextOptions,
};

use mc_domain::model::message::Message;
use mc_domain::traits::search_index::IndexExtras;
use mc_domain::value_objects::flag::Flag;

pub(super) struct IndexFields {
    pub message_id: Field,
    pub account_id: Field,
    /// Every mailbox the message is in.
    pub mailbox_id: Field,
    pub date: Field,
    pub flags: Field,
    pub has_attachment: Field,
    pub subject: Field,
    pub from_name: Field,
    pub body: Field,
    pub from_email: Field,
    pub to: Field,
    pub cc: Field,
    /// Lowercased label paths plus every parent path ("dev/github", "dev").
    pub labels: Field,
    /// `unread`, `read`, `starred`, `answered`, `attachment`: the yes/no
    /// filters, as terms so they combine with everything else in one query.
    pub state: Field,
    pub list_id: Field,
    pub attachment: Field,
}

impl IndexFields {
    pub fn of(schema: &Schema) -> Self {
        let field = |name| schema.get_field(name).expect("field is in build_schema");
        Self {
            message_id: field("message_id"),
            account_id: field("account_id"),
            mailbox_id: field("mailbox_id"),
            date: field("date"),
            flags: field("flags"),
            has_attachment: field("has_attachment"),
            subject: field("subject"),
            from_name: field("from_name"),
            body: field("body"),
            from_email: field("from_email"),
            to: field("to"),
            cc: field("cc"),
            labels: field("labels"),
            state: field("state"),
            list_id: field("list_id"),
            attachment: field("attachment"),
        }
    }
}

pub(super) fn build_schema() -> Schema {
    let mut builder = Schema::builder();
    let text = |tokenizer| {
        TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(tokenizer)
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        )
    };

    builder.add_text_field("message_id", STRING | STORED);
    builder.add_text_field("account_id", STRING | FAST);
    builder.add_text_field("mailbox_id", STRING | FAST);
    builder.add_i64_field("date", INDEXED | FAST);
    builder.add_u64_field("flags", FAST);
    builder.add_bool_field("has_attachment", FAST);
    for name in ["subject", "from_name", "body"] {
        builder.add_text_field(name, text("en_stem"));
    }
    for name in ["from_email", "to", "cc", "list_id", "attachment"] {
        builder.add_text_field(name, text("email_tokenizer"));
    }
    builder.add_text_field("labels", STRING);
    builder.add_text_field("state", STRING);
    builder.build()
}

pub(super) fn message_to_document(
    f: &IndexFields,
    message: &Message,
    extras: &IndexExtras,
) -> TantivyDocument {
    let e = &message.envelope;
    let join = |names: Vec<&str>| names.join(" ");
    let mut doc = TantivyDocument::new();
    doc.add_text(f.message_id, message.id.to_string());
    doc.add_text(f.account_id, message.account_id.to_string());
    for membership in &message.memberships {
        doc.add_text(f.mailbox_id, membership.mailbox_id.to_string());
    }
    doc.add_i64(f.date, e.date.timestamp());
    doc.add_u64(f.flags, flag_bits(&message.flags));
    doc.add_bool(f.has_attachment, message.has_attachments);
    doc.add_text(f.subject, &e.subject);
    doc.add_text(
        f.from_name,
        join(e.from.iter().filter_map(|a| a.name.as_deref()).collect()),
    );
    doc.add_text(
        f.from_email,
        join(e.from.iter().map(|a| a.email.as_str()).collect()),
    );
    doc.add_text(f.to, join(e.to.iter().map(|a| a.email.as_str()).collect()));
    doc.add_text(f.cc, join(e.cc.iter().map(|a| a.email.as_str()).collect()));
    if let Some(body) = message.plain_body() {
        doc.add_text(f.body, body);
    }
    for key in extras.labels.iter().flat_map(|l| label_keys(l)) {
        doc.add_text(f.labels, key);
    }
    for state in states(message) {
        doc.add_text(f.state, state);
    }
    if let Some(list_id) = &extras.list_id {
        doc.add_text(f.list_id, list_id);
    }
    for name in message
        .attachments
        .iter()
        .filter_map(|a| a.filename.as_deref())
    {
        doc.add_text(f.attachment, name);
    }
    doc
}

/// The label's key (lowercased, as tagging keys labels) and each parent's.
pub(super) fn label_keys(label: &str) -> Vec<String> {
    let key = label_key(label);
    let mut keys = vec![key.clone()];
    let mut parent = key.as_str();
    while let Some((up, _)) = parent.rsplit_once('/') {
        keys.push(up.to_owned());
        parent = up;
    }
    keys
}

pub(super) fn label_key(label: &str) -> String {
    label
        .split('/')
        .map(str::trim)
        .collect::<Vec<_>>()
        .join("/")
        .to_lowercase()
}

fn states(message: &Message) -> Vec<&'static str> {
    let has = |flag: &Flag| message.flags.contains(flag);
    let mut states = vec![if has(&Flag::Seen) { "read" } else { "unread" }];
    if has(&Flag::Flagged) {
        states.push("starred");
    }
    if has(&Flag::Answered) {
        states.push("answered");
    }
    if message.has_attachments {
        states.push("attachment");
    }
    states
}

/// System flags as bits for the `flags` FAST field. Kept for the columnar
/// data; the filters read the `state` terms.
fn flag_bits(flags: &[Flag]) -> u64 {
    flags.iter().fold(0, |bits, flag| {
        bits | match flag {
            Flag::Seen => 1,
            Flag::Answered => 1 << 1,
            Flag::Flagged => 1 << 2,
            Flag::Deleted => 1 << 3,
            Flag::Draft => 1 << 4,
            Flag::Recent => 1 << 5,
            Flag::Custom(_) => 0,
        }
    })
}
