use calcard::vcard::{VCard, VCardEntry, VCardParameterName, VCardParameterValue, VCardProperty};
use chrono::{DateTime, Utc};

use super::{Doc, content_line, escape, invalid, timestamp};
use crate::error::DomainError;
use crate::model::contact::{ContactEmail, ContactEmailLabel};
use crate::value_objects::email_address::EmailAddress;

/// What queries need from a vCard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCard {
    pub uid: Option<String>,
    pub display_name: String,
    pub emails: Vec<ContactEmail>,
    pub phones: Vec<String>,
    pub organization: Option<String>,
    pub notes: Option<String>,
    /// `REV`: the remote side of last-write-wins.
    pub revised: Option<DateTime<Utc>>,
}

pub fn parse_vcard(raw: &str) -> Result<ParsedCard, DomainError> {
    let card = VCard::parse(raw).map_err(|_| invalid("vCard"))?;
    let text = |property: &VCardProperty| {
        card.property(property)
            .and_then(|e| e.values.first())
            .and_then(|v| v.as_text())
            .map(str::to_owned)
    };

    let emails: Vec<ContactEmail> = texts(&card, &VCardProperty::Email)
        .into_iter()
        .filter_map(|(entry, address)| {
            Some(ContactEmail::new(
                EmailAddress::new(address.trim()).ok()?,
                label(entry),
                is_preferred(entry),
            ))
        })
        .collect();
    let display_name = text(&VCardProperty::Fn)
        .filter(|name| !name.trim().is_empty())
        .or_else(|| structured_name(&card))
        .or_else(|| emails.first().map(|e| e.address.as_str().to_owned()))
        .unwrap_or_default();

    Ok(ParsedCard {
        uid: text(&VCardProperty::Uid),
        display_name,
        emails,
        phones: texts(&card, &VCardProperty::Tel)
            .into_iter()
            .map(|(_, t)| t)
            .collect(),
        organization: text(&VCardProperty::Org).filter(|org| !org.is_empty()),
        notes: text(&VCardProperty::Note),
        revised: card
            .property(&VCardProperty::Rev)
            .and_then(|e| e.values.first()?.as_partial_date_time()?.to_timestamp())
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0)),
    })
}

fn texts<'a>(card: &'a VCard, property: &'a VCardProperty) -> Vec<(&'a VCardEntry, String)> {
    card.properties(property)
        .filter_map(|e| Some((e, e.values.first()?.as_text()?.to_owned())))
        .collect()
}

fn structured_name(card: &VCard) -> Option<String> {
    let parts: Vec<&str> = card
        .property(&VCardProperty::N)?
        .values
        .iter()
        .filter_map(|v| v.as_text())
        .filter(|part| !part.is_empty())
        .collect();
    // N is family;given;additional;prefix;suffix, shown given first.
    let name = match parts.as_slice() {
        [family, given, ..] => format!("{given} {family}"),
        [only] => (*only).to_owned(),
        [] => return None,
    };
    Some(name)
}

fn type_names(entry: &VCardEntry) -> Vec<String> {
    entry
        .params
        .iter()
        .filter(|p| p.name == VCardParameterName::Type)
        .flat_map(|p| match &p.value {
            VCardParameterValue::Type(t) => vec![format!("{t:?}")],
            VCardParameterValue::Text(t) => t.split(',').map(str::to_owned).collect(),
            _ => vec![],
        })
        .map(|name| name.to_ascii_lowercase())
        .collect()
}

fn label(entry: &VCardEntry) -> ContactEmailLabel {
    let names = type_names(entry);
    if names.iter().any(|n| n == "work") {
        ContactEmailLabel::Work
    } else if names.iter().any(|n| n == "home") {
        ContactEmailLabel::Home
    } else {
        ContactEmailLabel::Other
    }
}

fn is_preferred(entry: &VCardEntry) -> bool {
    entry
        .params
        .iter()
        .any(|p| p.name == VCardParameterName::Pref)
        || type_names(entry).iter().any(|n| n == "pref")
}

/// A change to a contact's vCard; the fields mirror `ContactUpdated`.
#[derive(Debug, Clone, Default)]
pub struct ContactEdit {
    pub display_name: Option<String>,
    pub emails: Option<Vec<ContactEmail>>,
    pub phones: Option<Vec<String>>,
    /// `Some(None)` clears it.
    pub organization: Option<Option<String>>,
    pub notes: Option<Option<String>>,
}

/// Applies `edit` to the vCard text and stamps `REV`. Every other property is
/// kept as written. An email or phone that stays in the list keeps its line,
/// parameters included.
pub fn edit_vcard(
    raw: &str,
    edit: &ContactEdit,
    now: DateTime<Utc>,
) -> Result<String, DomainError> {
    let mut doc = Doc::parse(raw);
    let first = card_scope(&doc).ok_or_else(|| invalid("vCard"))?;
    let v4 = doc
        .first(&first, "VERSION")
        .is_some_and(|v| v.value().starts_with('4'));

    let single = |doc: &mut Doc, name: &str, value: Option<Option<&String>>| {
        let Some(value) = value else { return };
        let scope = card_scope(doc).expect("checked above");
        doc.replace(&scope, name, |_| {
            value
                .map(|v| content_line(name, "", &escape(v)))
                .into_iter()
                .collect()
        });
    };
    single(&mut doc, "FN", edit.display_name.as_ref().map(Some));
    single(
        &mut doc,
        "ORG",
        edit.organization.as_ref().map(Option::as_ref),
    );
    single(&mut doc, "NOTE", edit.notes.as_ref().map(Option::as_ref));

    if let Some(emails) = &edit.emails {
        let scope = card_scope(&doc).expect("checked above");
        doc.replace(&scope, "EMAIL", |old| {
            emails
                .iter()
                .map(|email| {
                    let address = email.address.as_str();
                    old.iter()
                        .find(|l| l.value().eq_ignore_ascii_case(address))
                        .map_or_else(
                            || content_line("EMAIL", &email_params(email, v4), address),
                            |l| l.text.clone(),
                        )
                })
                .collect()
        });
    }
    if let Some(phones) = &edit.phones {
        let scope = card_scope(&doc).expect("checked above");
        doc.replace(&scope, "TEL", |old| {
            phones
                .iter()
                .map(|phone| {
                    let value = escape(phone);
                    old.iter()
                        .find(|l| l.value() == value)
                        .map_or_else(|| content_line("TEL", "", &value), |l| l.text.clone())
                })
                .collect()
        });
    }

    let scope = card_scope(&doc).expect("checked above");
    doc.replace(&scope, "REV", |_| {
        vec![content_line("REV", "", &timestamp(now))]
    });
    Ok(doc.render())
}

fn card_scope(doc: &Doc) -> Option<std::ops::Range<usize>> {
    doc.components("VCARD").into_iter().next()
}

fn email_params(email: &ContactEmail, v4: bool) -> String {
    let kind = match email.label {
        ContactEmailLabel::Work => Some("work"),
        ContactEmailLabel::Home => Some("home"),
        _ => None,
    };
    match (kind, email.is_primary, v4) {
        (None, false, _) => String::new(),
        (Some(k), false, _) => format!(";TYPE={k}"),
        (None, true, true) => ";PREF=1".into(),
        (None, true, false) => ";TYPE=pref".into(),
        (Some(k), true, true) => format!(";TYPE={k};PREF=1"),
        (Some(k), true, false) => format!(";TYPE={k},pref"),
    }
}
