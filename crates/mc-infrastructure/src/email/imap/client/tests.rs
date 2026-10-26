use super::*;

mod commands;
mod fetch_and_connect;

type FakeSession = async_imap::Session<tokio_util::compat::Compat<tokio::io::DuplexStream>>;

/// A logged-in session against an in-memory server that answers each
/// command with `reply(command)` (`$T` = its tag) and records the commands.
async fn scripted(reply: fn(&str) -> &'static str) -> (FakeSession, Arc<Mutex<Vec<String>>>) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio_util::compat::TokioAsyncReadCompatExt;

    let (client_end, server_end) = tokio::io::duplex(64 * 1024);
    let commands = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&commands);
    tokio::spawn(async move {
        let (read, mut write) = tokio::io::split(server_end);
        write.write_all(b"* OK ready\r\n").await.unwrap();
        let mut lines = BufReader::new(read).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let (tag, command) = line.split_once(' ').unwrap();
            seen.lock().unwrap().push(command.to_owned());
            let answer = if command.starts_with("LOGIN") {
                "$T OK logged in\r\n"
            } else {
                reply(command)
            };
            write
                .write_all(answer.replace("$T", tag).as_bytes())
                .await
                .unwrap();
        }
    });

    let mut client = async_imap::Client::new(client_end.compat());
    client.read_response().await.unwrap();
    let session = client.login("user", "pass").await.map_err(|e| e.0).unwrap();
    (session, commands)
}

fn uid_commands(commands: &Mutex<Vec<String>>) -> Vec<String> {
    commands
        .lock()
        .unwrap()
        .iter()
        .filter(|c| !c.starts_with("LOGIN"))
        .cloned()
        .collect()
}

fn server_reply(command: &str) -> &'static str {
    if command.starts_with("UID MOVE") {
        "* OK [COPYUID 7 5 77] Moved\r\n* 3 EXPUNGE\r\n$T OK Done\r\n"
    } else if command.starts_with("UID COPY") {
        "$T OK [COPYUID 7 5 78] Done\r\n"
    } else if command.contains("EXPUNGE") {
        "* 3 EXPUNGE\r\n$T OK Done\r\n"
    } else {
        "$T OK Done\r\n"
    }
}

/// The UIDs of a `UID FETCH <set> ...` command.
fn fetched_uids(command: &str) -> Vec<u32> {
    let set = command.split(' ').nth(2).unwrap();
    set.split(',')
        .flat_map(|range| match range.split_once(':') {
            Some((a, b)) => (a.parse().unwrap()..=b.parse().unwrap()).collect::<Vec<u32>>(),
            None => vec![range.parse().unwrap()],
        })
        .collect()
}
