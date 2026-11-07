//! WebDAV request bodies and the multistatus parser.
//!
//! Elements are matched by local name only. The properties asked for never
//! clash across namespaces, and servers disagree on prefixes (Google answers
//! `D:`, Radicale `ns0:`), so resolving namespaces would buy nothing.

use std::collections::HashMap;

use quick_xml::escape::escape;
use quick_xml::events::Event;
use quick_xml::reader::Reader;

use mc_domain::error::DomainError;

use super::Kind;

const NAMESPACES: &str = r#"xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:A="urn:ietf:params:xml:ns:carddav" xmlns:CS="http://calendarserver.org/ns/" xmlns:I="http://apple.com/ns/ical/""#;

fn propfind(props: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="utf-8"?><D:propfind {NAMESPACES}><D:prop>{props}</D:prop></D:propfind>"#
    )
}

/// What a collection says about itself: the cheap-check pair plus identity.
pub(super) fn head_props() -> String {
    propfind("<CS:getctag/><D:sync-token/>")
}

pub(super) fn etag_listing() -> String {
    propfind("<D:getetag/><D:resourcetype/>")
}

pub(super) fn principal_and_home(kind: Kind) -> String {
    let home = match kind {
        Kind::Contacts => "<A:addressbook-home-set/>",
        Kind::Calendar => "<C:calendar-home-set/>",
    };
    propfind(&format!(
        "<D:current-user-principal/><D:resourcetype/>{home}"
    ))
}

pub(super) fn collection_listing(kind: Kind) -> String {
    let specific = match kind {
        Kind::Contacts => "<A:addressbook-description/>",
        Kind::Calendar => {
            "<I:calendar-color/><C:calendar-timezone/><C:supported-calendar-component-set/><CS:source/>"
        }
    };
    propfind(&format!(
        "<D:displayname/><D:resourcetype/><CS:getctag/><D:sync-token/><D:owner/>\
         <D:current-user-privilege-set/><D:current-user-principal/>{specific}"
    ))
}

pub(super) fn sync_collection(token: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="utf-8"?><D:sync-collection xmlns:D="DAV:"><D:sync-token>{}</D:sync-token><D:sync-level>1</D:sync-level><D:prop><D:getetag/></D:prop></D:sync-collection>"#,
        escape(token)
    )
}

pub(super) fn multiget<'a>(kind: Kind, hrefs: impl Iterator<Item = &'a str>) -> String {
    let (root, data) = match kind {
        Kind::Contacts => ("A:addressbook-multiget", "<A:address-data/>"),
        Kind::Calendar => ("C:calendar-multiget", "<C:calendar-data/>"),
    };
    let hrefs: String = hrefs
        .map(|h| format!("<D:href>{}</D:href>", escape(h)))
        .collect();
    format!(
        r#"<?xml version="1.0" encoding="utf-8"?><{root} {NAMESPACES}><D:prop><D:getetag/>{data}</D:prop>{hrefs}</{root}>"#
    )
}

/// One property's value, flattened: servers nest the same few shapes
/// (`<owner><href>`, `<resourcetype><calendar/>`, `<privilege><write/>`).
#[derive(Debug, Default, Clone)]
pub(super) struct Prop {
    pub text: String,
    /// Local names of every element below the property.
    pub tags: Vec<String>,
    pub hrefs: Vec<String>,
    /// `name` attributes of descendants: `<comp name="VEVENT"/>`.
    pub names: Vec<String>,
}

impl Prop {
    pub fn has(&self, tag: &str) -> bool {
        self.tags.iter().any(|t| t == tag)
    }
}

#[derive(Debug, Default)]
pub(super) struct Response {
    pub href: String,
    /// Set when the whole response carries a status, as sync-collection does
    /// for a removed resource (404).
    pub status: Option<u16>,
    /// Only properties the server found (a 2xx propstat).
    pub props: HashMap<String, Prop>,
}

impl Response {
    pub fn text(&self, name: &str) -> Option<&str> {
        let text = self.props.get(name)?.text.as_str();
        (!text.is_empty()).then_some(text)
    }

    pub fn is_collection(&self) -> bool {
        self.props
            .get("resourcetype")
            .is_some_and(|p| p.has("collection"))
    }
}

#[derive(Debug, Default)]
pub(super) struct Multistatus {
    pub responses: Vec<Response>,
    pub sync_token: Option<String>,
}

fn status_code(line: &str) -> Option<u16> {
    line.split_whitespace().nth(1)?.parse().ok()
}

/// Payload properties keep their whitespace: a trailing newline is part of
/// an iCalendar body, and the server's text is what we write back.
const VERBATIM: [&str; 3] = ["calendar-data", "address-data", "calendar-timezone"];

#[derive(Default)]
struct Parser {
    out: Multistatus,
    path: Vec<String>,
    response: Response,
    propstat: HashMap<String, Prop>,
    propstat_status: Option<u16>,
    prop_name: String,
    prop: Prop,
    status: String,
    token: String,
}

impl Parser {
    fn in_prop(&self) -> bool {
        self.path.len() >= 4 && self.path[2] == "propstat" && self.path[3] == "prop"
    }

    fn start(&mut self, name: String, attrs: Vec<String>) {
        if self.in_prop() {
            if self.path.len() == 4 {
                self.prop_name.clone_from(&name);
                self.prop = Prop::default();
            } else {
                self.prop.tags.push(name);
                self.prop.names.extend(attrs);
            }
        }
    }

    fn text(&mut self, text: &str) {
        let p: Vec<&str> = self.path.iter().map(String::as_str).collect();
        match p.as_slice() {
            ["multistatus", "response", "href"] => self.response.href.push_str(text.trim()),
            ["multistatus", "response", "status"]
            | ["multistatus", "response", "propstat", "status"] => self.status.push_str(text),
            ["multistatus", "sync-token"] => self.token.push_str(text.trim()),
            ["multistatus", "response", "propstat", "prop", ..] if p.len() >= 5 => {
                self.prop.text.push_str(text);
                if p[p.len() - 1] == "href" {
                    self.prop.hrefs.push(text.trim().to_owned());
                }
            }
            _ => {}
        }
    }

    fn end(&mut self, name: &str) {
        let depth = self.path.len();
        self.path.pop();
        match (depth, name) {
            (5, _) if self.in_prop() => {
                let mut prop = std::mem::take(&mut self.prop);
                if !VERBATIM.contains(&self.prop_name.as_str()) {
                    prop.text = prop.text.trim().to_owned();
                }
                self.propstat
                    .insert(std::mem::take(&mut self.prop_name), prop);
            }
            (3, "propstat") => {
                let ok = self
                    .propstat_status
                    .take()
                    .is_none_or(|s| (200..300).contains(&s));
                if ok {
                    self.response.props.extend(self.propstat.drain());
                }
                self.propstat.clear();
            }
            (3, "status") | (4, "status") => {
                let code = status_code(&self.status);
                self.status.clear();
                if depth == 4 {
                    self.propstat_status = code;
                } else {
                    self.response.status = code;
                }
            }
            (2, "response") => self.out.responses.push(std::mem::take(&mut self.response)),
            (2, "sync-token") => {
                self.out.sync_token =
                    Some(std::mem::take(&mut self.token)).filter(|t| !t.is_empty());
            }
            _ => {}
        }
    }
}

pub(super) fn parse_multistatus(xml: &[u8]) -> Result<Multistatus, DomainError> {
    let bad = |e: &dyn std::fmt::Display| DomainError::Provider(format!("DAV multistatus: {e}"));
    let mut reader = Reader::from_reader(xml);
    let mut parser = Parser::default();
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf).map_err(|e| bad(&e))? {
            event @ (Event::Start(_) | Event::Empty(_)) => {
                let (empty, e) = match &event {
                    Event::Empty(e) => (true, e),
                    Event::Start(e) => (false, e),
                    _ => unreachable!(),
                };
                let name = e.local_name().as_ref().to_owned();
                let attrs = e
                    .attributes()
                    .flatten()
                    .filter(|a| a.key.local_name().as_ref() == "name")
                    .map(|a| a.value.into_owned())
                    .collect();
                parser.start(name.clone(), attrs);
                parser.path.push(name.clone());
                if empty {
                    parser.end(&name);
                }
            }
            Event::End(e) => {
                let name = e.local_name().as_ref().to_owned();
                parser.end(&name);
            }
            Event::Text(t) => parser.text(&t.xml10_content()),
            Event::CData(t) => parser.text(&t.xml10_content()),
            Event::GeneralRef(r) => {
                // Entities arrive as events of their own: iCalendar line
                // breaks are often sent as `&#13;`.
                let decoded = match r.resolve_char_ref().map_err(|e| bad(&e))? {
                    Some(c) => c.to_string(),
                    None => match &*r {
                        "amp" => "&".into(),
                        "lt" => "<".into(),
                        "gt" => ">".into(),
                        "quot" => "\"".into(),
                        "apos" => "'".into(),
                        other => return Err(bad(&format!("unknown entity &{other};"))),
                    },
                };
                parser.text(&decoded);
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    Ok(parser.out)
}
