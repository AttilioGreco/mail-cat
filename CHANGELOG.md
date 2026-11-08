# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0-rc.2] - 2026-11-08

### Added
- Contacts and calendars: two-way CardDAV/CalDAV sync (Google included), with a calendar section, agenda pane, contact card and contacts section.
- Subscribe to colleagues' calendars.
- Calendar reminders as desktop notifications.
- Sync contacts and calendars on demand (button, F9), with the last sync time.
- Per-feature contacts/calendar permissions, with re-authorization from settings.
- `mc` CLI: contacts and calendars commands.

### Changed
- The server no longer listens on a network port: the app, the CLI and the server talk over a local socket (Unix socket, named pipe on Windows), usable only by the user who runs the server.
- Config key `grpc_addr` replaced by the optional `socket_path`; `MAILCAT_SERVER_URL` replaced by `MAILCAT_SOCKET`, and `mc --server` by `mc --socket`.

### Fixed
- Calendar events no longer shift by an hour on DST days.
- Recurring series split by the server are read correctly, and unreadable events are kept.
- A local contact/event edit is never overwritten while a sync conflict is being resolved.

## [0.1.3-rc.3] - 2026-11-07

## [0.1.3-rc.2] - 2026-11-07

## [0.1.3-rc.1] - 2026-11-07

## [0.1.2] - 2026-11-06

## [0.1.1] - 2026-11-06

## [0.1.0] - 2026-11-06

First public release.

### Added
- IMAP and JMAP accounts, Gmail and Microsoft through OAuth, with background sync that keeps running when the window is closed.
- Desktop app for Linux (AppImage, deb, rpm), macOS (Apple Silicon) and Windows, with the server and the `mc` CLI bundled.
- In-app updates: the app shows when a new version is available and installs it on confirmation.
- The server starts automatically at login (systemd user unit, LaunchAgent, Scheduled Task).
- Visual HTML compose editor, markdown as an advanced mode.
- Labels: suggestions and auto-labelling (naive Bayes plus local embeddings), discovery of new labels, rename/move/merge/delete across accounts, label by sender, labels page with colors.
- Search with Gmail-style operators (`from:`, `is:unread`, `filename:`, labels).
- `mc` CLI: accounts, mailboxes, label curation plans with undo.
