//! The JSON these commands print is a contract for assistants: field names
//! and shapes are pinned here (and documented in docs/mc-labels-cli.md).

use clap::{CommandFactory, Parser};
use mc_client::{
    ApplyAllReport, ApplyPreviewItem, LabelApply, LabelEntry, LabelProposal, LabelScope,
    LabelStatus, MatchAccount, MatchReport, ProposalDetail, ProposalMessage, ProposalSample,
    SenderCount,
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
        curated: true,
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
            "curated": true,
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
    assert!(parses(&["mode", "news", "suggest"]));
}

#[test]
fn proposals_toon_snapshot() {
    let mut other = proposal();
    other.id = "0198d1a2-0000-7000-8000-000000000002".into();
    other.name = "Shop, \"Ebay\"".into();
    other.keywords = vec!["ebay".into()];
    other.senders.clear();
    other.curated = false;
    assert_eq!(
        super::render::proposals(&[proposal(), other]).as_str(),
        "proposals[2]{id,name,size,inbox,curated,status,group,keywords,senders}:\n\
         \x20 0198d1a2-0000-7000-8000-000000000001,GitHub,120,100,true,pending,Tutti,github|pull,github.com(118)\n\
         \x20 0198d1a2-0000-7000-8000-000000000002,\"Shop, \\\"Ebay\\\"\",120,100,false,pending,Tutti,ebay,\"\"\n"
    );
}

#[test]
fn the_new_commands_parse() {
    assert!(parses(&["proposals", "plan", "plan.json"]));
    assert!(parses(&["proposals", "plan", "plan.json", "--dry-run"]));
    assert!(parses(&["proposals", "plan", "plan.json", "--yes"]));
    assert!(!parses(&["proposals", "plan"]));
    assert!(parses(&["proposals", "undo", ID, "--yes", "--reject"]));
    assert!(parses(&[
        "rename",
        "Github",
        "Dev/GitHub",
        "--account",
        "a",
        "--yes"
    ]));
    assert!(!parses(&["rename", "Github"]));
    assert!(parses(&["delete", "medium", "--yes"]));
    assert!(!parses(&["delete"]));
    assert!(parses(&[
        "show",
        "Shopping/Ebay",
        "--account",
        "a",
        "--samples",
        "3"
    ]));
}

fn item() -> ApplyPreviewItem {
    ApplyPreviewItem {
        message_id: "m1".into(),
        subject: "Il tuo preventivo".into(),
        from: "Facile".into(),
    }
}

#[test]
fn bulk_labelling_commands_parse() {
    assert!(parses(&[
        "assign-matching",
        "Casa/Facile",
        "--from",
        "facile.it",
        "--from",
        "a@b.it",
        "--list-id",
        "news.example",
        "--archive",
        "--yes"
    ]));
    assert!(parses(&["assign-matching", "Casa"]));
    assert!(parses(&[
        "apply-all",
        "--label",
        "a",
        "--label",
        "b",
        "--yes"
    ]));
    assert!(parses(&["apply-all"]));
    assert!(!parses(&["assign-matching"]));
}

#[test]
fn assign_matching_json_and_toon() {
    let report = MatchReport {
        dry_run: true,
        label: "Casa/Facile".into(),
        to_label: 93,
        to_archive: 10,
        already: 2,
        creates: true,
        accounts: vec![MatchAccount {
            account_id: "a1".into(),
            messages: 93,
            archived: 10,
            already: 2,
            creates: true,
        }],
        sample: vec![item()],
    };
    assert_eq!(
        serde_json::to_value(&report).unwrap(),
        json!({
            "dryRun": true, "label": "Casa/Facile", "toLabel": 93, "toArchive": 10,
            "already": 2, "creates": true,
            "accounts": [{"accountId": "a1", "messages": 93, "archived": 10, "already": 2, "creates": true}],
            "sample": [{"messageId": "m1", "subject": "Il tuo preventivo", "from": "Facile"}],
        })
    );
    assert_eq!(
        super::render::match_report(&report).as_str(),
        "dryRun: true\nlabel: Casa/Facile\ntoLabel: 93\ntoArchive: 10\nalready: 2\ncreates: true\n\
         accounts[1]{accountId,messages,archived,already,creates}:\n\x20 a1,93,10,2,true\n\
         sample[1]{messageId,from,subject}:\n\x20 m1,Facile,Il tuo preventivo\n"
    );
}

#[test]
fn apply_all_json_and_toon() {
    let report = ApplyAllReport {
        dry_run: false,
        to_label: 40,
        labels: vec![LabelApply {
            scope: "g:1".into(),
            name: "Banca".into(),
            label: "banca".into(),
            mode: "suggest".into(),
            threshold: 0.9,
            positives: 12,
            to_label: 40,
            sample: vec![item()],
        }],
    };
    assert_eq!(
        serde_json::to_value(&report).unwrap(),
        json!({
            "dryRun": false, "toLabel": 40,
            "labels": [{
                "scope": "g:1", "name": "Banca", "label": "banca", "mode": "suggest",
                "threshold": 0.9, "positives": 12, "toLabel": 40,
                "sample": [{"messageId": "m1", "subject": "Il tuo preventivo", "from": "Facile"}],
            }],
        })
    );
    assert_eq!(
        super::render::apply_all(&report).as_str(),
        "dryRun: false\ntoLabel: 40\n\
         labels[1]{name,label,mode,threshold,positives,toLabel,scope}:\n\
         \x20 Banca,banca,suggest,0.9,12,40,\"g:1\"\n"
    );
}
