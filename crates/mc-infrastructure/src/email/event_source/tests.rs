use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;

/// Counts the watch sessions still open.
struct Session(Arc<AtomicUsize>);

impl Session {
    fn open(open: &Arc<AtomicUsize>) -> Self {
        open.fetch_add(1, Ordering::SeqCst);
        Self(open.clone())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn dropping_the_stream_closes_every_watch_session_at_once() {
    let open_sessions = Arc::new(AtomicUsize::new(0));
    let counter = open_sessions.clone();
    // Quiet sessions: a task that only stopped on its next send would never end.
    let mut merged = merge_watches(
        vec![
            MailboxPath::inbox(),
            MailboxPath::new(vec!["Archive".into()], '/').unwrap(),
        ],
        move |_| {
            let session = Session::open(&counter);
            Box::pin(async_stream::stream! {
                let _session = session;
                futures::future::pending::<()>().await;
                yield Err(DomainError::Validation(String::new()));
            })
        },
    );
    // First poll starts the tasks.
    let _ = tokio::time::timeout(Duration::from_millis(100), merged.next()).await;
    assert_eq!(open_sessions.load(Ordering::SeqCst), 2);

    drop(merged);

    tokio::time::timeout(Duration::from_secs(2), async {
        while open_sessions.load(Ordering::SeqCst) > 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("watch sessions outlived the stream");
}
