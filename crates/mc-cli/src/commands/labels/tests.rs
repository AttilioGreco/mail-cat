//! The JSON these commands print is a contract for assistants: field names
//! and shapes are pinned here (and documented in docs/mc-labels-cli.md).

use clap::{CommandFactory, Parser};
use mc_client::{
    LabelEntry, LabelProposal, LabelScope, LabelStatus, ProposalDetail, ProposalMessage,
    ProposalSample, SenderCount,
};
use serde_json::json;

use super::LabelsCmd;
use super::proposals::{ModeArg, is_dry_run};

const ID: &str = "0198d1a2-0000-7000-8000-000000000001";

fn proposal() -> LabelProposal {
    LabelProposal {
        id: ID.into(),
        name: "GitHub".into(),
        keywords: vec!["github".into(), "pull".into()],
        size: 120,
        inbox: 100,
        samples: vec![ProposalSample {
            message_id: "m1".into(),
            subject: "Re: fix".into(),
            from: "octocat".into(),
        }],
        group: "Tutti".into(),
        created_at: 1_700_000_000,
        status: "pending".into(),
        scope: "g:00000000-0000-0000-0000-000000000001".into(),
        senders: vec![SenderCount {
            domain: "github.com".into(),
            count: 118,
        }],
    }
}

#[test]
fn proposals_json_shape() {
    assert_eq!(
        serde_json::to_value(vec![proposal()]).unwrap(),
        json!([{
            "id": ID,
            "name": "GitHub",
            "keywords": ["github", "pull"],
            "size": 120,
            "inbox": 100,
            "samples": [{"messageId": "m1", "subject": "Re: fix", "from": "octocat"}],
            "group": "Tutti",
            "createdAt": 1_700_000_000,
            "status": "pending",
            "scope": "g:00000000-0000-0000-0000-000000000001",
            "senders": [{"domain": "github.com", "count": 118}],
        }])
    );
}

#[test]
fn proposal_detail_json_adds_messages_to_the_proposal_fields() {
    let detail = ProposalDetail {
        proposal: proposal(),
        messages: vec![ProposalMessage {
            message_id: "m1".into(),
            received_at: "2026-10-01T08:00:00+00:00".into(),
            from: "octocat".into(),
            subject: "Re: fix".into(),
            excerpt: "Merged".into(),
        }],
    };
    let value = serde_json::to_value(detail).unwrap();
    assert_eq!(value["id"], ID);
    assert_eq!(
        value["messages"],
        json!([{
            "messageId": "m1",
            "receivedAt": "2026-10-01T08:00:00+00:00",
            "from": "octocat",
            "subject": "Re: fix",
            "excerpt": "Merged",
        }])
    );
}

#[test]
fn labels_list_json_shape() {
    let scopes = vec![LabelScope {
        scope: "g:00000000-0000-0000-0000-000000000001".into(),
        group: "Tutti".into(),
        account_ids: vec!["acc".into()],
        labels: vec![LabelEntry {
            name: "Newsletter".into(),
            status: LabelStatus {
                label: "newsletter".into(),
                mode: "suggest".into(),
                threshold: 0.9,
                positives: 42,
                min_positives: 5,
            },
        }],
    }];
    assert_eq!(
        serde_json::to_value(scopes).unwrap(),
        json!([{
            "scope": "g:00000000-0000-0000-0000-000000000001",
            "group": "Tutti",
            "accountIds": ["acc"],
            "labels": [{
                "name": "Newsletter",
                "label": "newsletter",
                "mode": "suggest",
                "threshold": 0.9,
                "positives": 42,
                "minPositives": 5,
            }],
        }])
    );
}

#[test]
fn only_apply_modes_wait_for_yes() {
    for (mode, yes, dry) in [
        (ModeArg::Off, false, false),
        (ModeArg::Suggest, false, false),
        (ModeArg::Apply, false, true),
        (ModeArg::ApplyArchive, false, true),
        (ModeArg::Apply, true, false),
        (ModeArg::ApplyArchive, true, false),
    ] {
        assert_eq!(is_dry_run(mode, yes), dry);
    }
}

#[derive(Parser)]
struct Wrapper {
    #[command(subcommand)]
    cmd: LabelsCmd,
}

fn parses(args: &[&str]) -> bool {
    Wrapper::try_parse_from(["mc"].into_iter().chain(args.iter().copied())).is_ok()
}

#[test]
fn the_command_line_is_well_formed() {
    Wrapper::command().debug_assert();
    assert!(parses(&["proposals"]));
    assert!(parses(&[
        "proposals",
        "--status",
        "all",
        "--account",
        "a@x.it"
    ]));
    assert!(parses(&["proposals", "show", ID, "--samples", "3"]));
    assert!(parses(&[
        "proposals",
        "merge",
        "a",
        "b",
        "c",
        "--name",
        "X"
    ]));
    assert!(!parses(&["proposals", "merge", "a"]));
    assert!(parses(&[
        "proposals",
        "assign",
        ID,
        "Newsletter",
        "--archive"
    ]));
    assert!(parses(&[
        "mode",
        "news",
        "apply_archive",
        "--existing",
        "--yes"
    ]));
    assert!(!parses(&["mode", "news", "sometimes"]));
}
