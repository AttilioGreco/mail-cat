fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = [
        "../../proto/mail_cat/v1/common.proto",
        "../../proto/mail_cat/v1/accounts.proto",
        "../../proto/mail_cat/v1/mailboxes.proto",
        "../../proto/mail_cat/v1/messages.proto",
        "../../proto/mail_cat/v1/search.proto",
        "../../proto/mail_cat/v1/contacts.proto",
    ];

    tonic_prost_build::configure()
        .build_server(false) // client-only
        .build_client(true)
        .compile_protos(&proto_files, &["../../proto"])?;

    for proto_file in &proto_files {
        println!("cargo:rerun-if-changed={}", proto_file);
    }

    Ok(())
}
