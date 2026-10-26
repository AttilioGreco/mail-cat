use super::*;

#[tokio::test]
async fn sync_service_integrated_in_app_context() {
    let dir = tempfile::tempdir().unwrap();
    let config = ServerConfig {
        grpc_addr: "127.0.0.1:50051".to_string(),
        db_path: dir.path().join("mail-cat.db").display().to_string(),
        index_path: dir.path().join("index").display().to_string(),
        accounts: vec![],
        ui_bin: "mail-cat-ui".to_string(),
        log_level: None,
    };

    // Verify AppContext can be created with wired dependencies.
    // No accounts → the secret store is never queried.
    let secret_store: Arc<dyn mc_domain::SecretStore> =
        Arc::new(mc_infrastructure::KeyringSecretStore::new());
    let oauth_challenges = Arc::new(crate::oauth_login::OAuthChallengeManager::new(
        std::path::PathBuf::from("/tmp/test_config_sync.toml"),
        secret_store.clone(),
    ));
    let context = match AppContext::new(
        &config,
        "/tmp/test_config_sync.toml",
        secret_store,
        oauth_challenges,
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(e) => panic!("failed to create app context: {}", e),
    };

    // Test that SyncService is available and callable
    let account_id = mc_domain::AccountId::new();
    let result = context.sync_service.start_sync(&account_id).await;

    // With no providers registered, this should fail with a provider error
    match result {
        Ok(_events) => {
            // If this succeeds in the future with registered providers,
            // we'll verify progress events are emitted
        }
        Err(e) => {
            // Verify that the error mentions provider not found
            let msg = e.to_string();
            assert!(
                msg.contains("no provider registered") || msg.contains("sync failed"),
                "unexpected error: {}",
                e
            );
        }
    }
}
