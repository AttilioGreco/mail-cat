use super::*;

#[test]
fn plain_text_stays_bare_and_accents_are_kept() {
    assert_eq!(s("GitHub"), "GitHub");
    assert_eq!(s("Fattura n 12 caffè"), "Fattura n 12 caffè");
    assert_eq!(s("Shopping/Ebay"), "Shopping/Ebay");
    assert_eq!(s("a b"), "a b");
}

#[test]
fn what_would_break_a_row_or_change_type_is_quoted_with_json_escapes() {
    for (raw, quoted) in [
        ("a,b", r#""a,b""#),
        ("Re: fix", r#""Re: fix""#),
        (r#"say "hi""#, r#""say \"hi\"""#),
        ("two\nlines", r#""two\nlines""#),
        (" lead", r#"" lead""#),
        ("trail ", r#""trail ""#),
        (r"back\slash", r#""back\\slash""#),
        ("[Gmail] Sent", r#""[Gmail] Sent""#),
        ("-dash", r#""-dash""#),
        ("", r#""""#),
        ("true", r#""true""#),
        ("null", r#""null""#),
        ("42", r#""42""#),
        ("1e3", r#""1e3""#),
        ("tab\there", r#""tab\there""#),
    ] {
        assert_eq!(s(raw), quoted, "{raw:?}");
    }
}

#[test]
fn a_list_in_a_cell_is_joined_with_pipes() {
    assert_eq!(l(["github", "pull"]), "github|pull");
    assert_eq!(l(Vec::<String>::new()), r#""""#);
    assert_eq!(l(["a,b", "c"]), r#""a,b|c""#);
}

#[test]
fn a_table_and_fields_make_a_snapshot() {
    let mut t = Toon::default();
    t.field("id", s("0198d1a2")).field("size", n(120));
    t.list("keywords", &["github", "pull request"]);
    t.table(
        "samples",
        &["messageId", "from", "subject"],
        &[
            vec![s("m1"), s("octocat"), s("Re: fix, again")],
            vec![s("m2"), s("Ada \"Countess\""), s("")],
        ],
    );
    t.table("empty", &["a"], &[]);
    assert_eq!(
        t.as_str(),
        "id: 0198d1a2\n\
         size: 120\n\
         keywords[2]: github,pull request\n\
         samples[2]{messageId,from,subject}:\n\
         \x20 m1,octocat,\"Re: fix, again\"\n\
         \x20 m2,\"Ada \\\"Countess\\\"\",\"\"\n\
         empty[0]{a}:\n"
    );
}
