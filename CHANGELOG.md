# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
