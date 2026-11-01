//! Putting a label on all the mail from some senders or mailing lists, for
//! mail discovery never grouped (it sat in a mixed cluster). The model learns
//! the label from it as from any label the user chose.

use std::collections::HashMap;
use std::sync::Arc;

use mc_domain::{
    AccountId, LabelMode, LabelSettings, MailboxReadRepository, Scope, TagSource, TaggingStore,
};

use super::Home;
use super::Pick;
use crate::bus::CommandBus;
use crate::error::ApplicationError;
use crate::services::tagging::retro::PreviewItem;
use crate::services::tagging::{Placement, TaggingService, label_key, label_mailbox, label_path};

const SAMPLE: usize = 5;

/// What picks a message: its sender (an address, or a domain that also
/// covers its subdomains) or the mailing list it came through.
struct Matcher {
    senders: Vec<String>,
    lists: Vec<String>,
}

fn normalized(values: &[String], what: impl Fn(&str) -> String) -> Vec<String> {
    values
        .iter()
        .map(|v| what(v.trim()))
        .filter(|v| !v.is_empty())
        .collect()
}

/// `Name <list.example.com>` and `<list.example.com>` both name the list.
fn list_name(id: &str) -> String {
    let inner = id
        .split_once('<')
        .and_then(|(_, rest)| rest.split_once('>'))
        .map_or(id, |(inner, _)| inner);
    inner.trim().to_lowercase()
}

impl Matcher {
    fn new(from: &[String], list_id: &[String]) -> Result<Self, ApplicationError> {
        let senders = normalized(from, |v| v.trim_start_matches('@').to_lowercase());
        let lists = normalized(list_id, list_name);
        if senders.is_empty() && lists.is_empty() {
            return Err(ApplicationError::PreconditionFailed(
                "name at least one sender, domain or list id".into(),
            ));
        }
        Ok(Self { senders, lists })
    }

    fn matches(&self, source: &TagSource) -> bool {
        let email = source.from_email.to_lowercase();
        let domain = email.rsplit_once('@').map_or(email.as_str(), |(_, d)| d);
        let sender = self.senders.iter().any(|s| {
            if s.contains('@') {
                *s == email
            } else {
                domain == s
                    || domain
                        .strip_suffix(s.as_str())
                        .is_some_and(|rest| rest.ends_with('.'))
            }
        });
        sender
            || source
                .list_headers
                .list_id
                .as_deref()
                .is_some_and(|id| self.lists.contains(&list_name(id)))
    }
}

/// What assigning would do on one account.
#[derive(Debug, Clone)]
pub struct MatchAccount {
    pub account: AccountId,
    /// To be labelled.
    pub messages: u32,
    /// Of those, leaving the Inbox.
    pub archived: u32,
    /// Matching mail that already carries the label.
    pub already: u32,
    /// The account lacks the label and will get it.
    pub creates: bool,
}

#[derive(Debug, Clone)]
pub struct MatchReport {
    pub dry_run: bool,
    /// The label as it is (or will be) spelled.
    pub label: String,
    pub to_label: u32,
    pub to_archive: u32,
    pub already: u32,
    /// Some account will create the label.
    pub creates: bool,
    pub accounts: Vec<MatchAccount>,
    pub sample: Vec<PreviewItem>,
}

impl<S, R, C> TaggingService<S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    /// Label the mail of `from` (addresses or domains) and `list_id` that is
    /// in the label's group, creating the label where an account lacks it.
    /// Trash, Spam, Drafts and mail that has the label are left alone. Without
    /// `dry_run` the labelling is queued like an acceptance.
    pub async fn assign_matching(
        self: &Arc<Self>,
        label: &str,
        from: &[String],
        list_id: &[String],
        account: Option<AccountId>,
        archive: bool,
        dry_run: bool,
    ) -> Result<MatchReport, ApplicationError> {
        let matcher = Matcher::new(from, list_id)?;
        let typed = label_path(label)?;
        let key = label_key(&typed);
        if !dry_run && self.is_applying(&key) {
            return Err(ApplicationError::PreconditionFailed(format!(
                "{key:?} is still being applied or undone: wait for it to finish"
            )));
        }
        let scope = self.scope_for_label(&key, account).await?;
        let accounts = self.store.scope_accounts(scope).await?;
        let group = self.group_mailboxes(scope).await?;
        let name = group
            .values()
            .find_map(|boxes| label_mailbox(boxes, &key))
            .map_or(typed, |m| m.display_path.clone());
        let mode = if archive {
            LabelMode::ApplyArchive
        } else {
            LabelMode::Apply
        };

        let mut per_account = HashMap::<AccountId, MatchAccount>::new();
        let (mut picks, mut sample) = (Vec::new(), Vec::new());
        let mut created = HashMap::new();
        let ids = self
            .store
            .candidates(&accounts, &matcher.senders, &matcher.lists)
            .await?;
        for chunk in ids.chunks(self.page as usize) {
            let page = self.store.sources(chunk).await?;
            for source in page.iter().filter(|s| matcher.matches(s)) {
                let entry = per_account
                    .entry(source.account_id)
                    .or_insert_with(|| MatchAccount {
                        account: source.account_id,
                        messages: 0,
                        archived: 0,
                        already: 0,
                        creates: false,
                    });
                if source.labels.contains(&key) {
                    entry.already += 1;
                    continue;
                }
                if !source.can_label() {
                    continue;
                }
                let mut place = Placement::of(source, &group, &key, mode);
                entry.creates |= matches!(place.home, Home::Create(_));
                entry.messages += 1;
                entry.archived += u32::from(place.archives());
                if sample.len() < SAMPLE {
                    sample.push(PreviewItem {
                        message_id: source.id,
                        subject: source.subject.clone(),
                        from: source
                            .from_name
                            .clone()
                            .unwrap_or_else(|| source.from_email.clone()),
                    });
                }
                if !dry_run {
                    self.create_home(&mut created, source.account_id, &mut place, &name)
                        .await?;
                    picks.push(Pick::new(source, place, 1.0));
                }
            }
        }

        let mut accounts: Vec<MatchAccount> = per_account.into_values().collect();
        accounts.sort_by_key(|a| *a.account.as_uuid());
        let report = MatchReport {
            dry_run,
            label: name,
            to_label: accounts.iter().map(|a| a.messages).sum(),
            to_archive: accounts.iter().map(|a| a.archived).sum(),
            already: accounts.iter().map(|a| a.already).sum(),
            creates: accounts.iter().any(|a| a.creates),
            accounts,
            sample,
        };
        if !picks.is_empty() {
            self.start_suggesting(scope, &key).await?;
            let service = Arc::clone(self);
            tokio::spawn(async move { service.run(picks, key, true, None).await });
        }
        Ok(report)
    }

    /// The group that has the label, else the account's; a new label needs the
    /// account when there are several groups to put it in.
    async fn scope_for_label(
        &self,
        key: &str,
        account: Option<AccountId>,
    ) -> Result<Scope, ApplicationError> {
        if let Some(account) = account {
            return Ok(self.store.scope_of(account).await?);
        }
        let scopes = self.store.scopes().await?;
        let mut known = Vec::new();
        for &scope in &scopes {
            let group = self.group_mailboxes(scope).await?;
            if self.label_known(scope, key, &group).await? {
                known.push(scope);
            }
        }
        match (known.as_slice(), scopes.as_slice()) {
            ([scope], _) | ([], [scope]) => Ok(*scope),
            ([], _) => Err(ApplicationError::PreconditionFailed(format!(
                "{key:?} is a new label and there are several groups: pass --account"
            ))),
            _ => Err(ApplicationError::PreconditionFailed(format!(
                "label {key:?} is in several groups: pass --account"
            ))),
        }
    }

    /// A label that is off starts in `suggest` once it has mail of the user's.
    pub(super) async fn start_suggesting(
        &self,
        scope: Scope,
        label: &str,
    ) -> Result<(), ApplicationError> {
        let settings = self.store.settings(scope).await?;
        if settings
            .iter()
            .all(|s| s.label != label || s.mode == LabelMode::Off)
        {
            let suggest = LabelSettings {
                mode: LabelMode::Suggest,
                ..LabelSettings::off(label)
            };
            self.store.put_settings(scope, &suggest).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn from(email: &str, list: Option<&str>) -> TagSource {
        let mut s = TagSource {
            id: Default::default(),
            account_id: AccountId::new(),
            from_email: email.into(),
            from_name: None,
            subject: String::new(),
            list_headers: Default::default(),
            body: None,
            labels: vec![],
            keywords: false,
            tags: false,
            folder: None,
            in_inbox: true,
            mailboxes: 1,
            spellings: vec![],
            received_at: chrono::Utc::now(),
        };
        s.list_headers.list_id = list.map(Into::into);
        s
    }

    fn matcher(from: &[&str], lists: &[&str]) -> Matcher {
        let own = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        Matcher::new(&own(from), &own(lists)).unwrap()
    }

    #[test]
    fn a_domain_covers_its_subdomains_but_not_look_alikes() {
        let m = matcher(&["facile.it", "Anna@Family.example"], &[]);
        for yes in [
            "a@facile.it",
            "preventivo@remind.facile.it",
            "anna@family.example",
        ] {
            assert!(m.matches(&from(yes, None)), "{yes}");
        }
        for no in [
            "a@notfacile.it",
            "a@facile.it.evil.example",
            "bob@family.example",
        ] {
            assert!(!m.matches(&from(no, None)), "{no}");
        }
    }

    #[test]
    fn a_list_id_matches_with_or_without_its_name_and_brackets() {
        let m = matcher(&[], &["<News.Example.COM>"]);
        assert!(m.matches(&from("x@y.example", Some("Weekly <news.example.com>"))));
        assert!(m.matches(&from("x@y.example", Some("news.example.com"))));
        assert!(!m.matches(&from("x@y.example", Some("<other.example.com>"))));
        assert!(!m.matches(&from("x@y.example", None)));
    }

    #[test]
    fn a_filter_is_required() {
        assert!(Matcher::new(&[" ".into()], &[]).is_err());
    }
}
