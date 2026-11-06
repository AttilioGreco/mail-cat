# Installing mail-cat

Download the file for your system from the [latest release](https://github.com/AttilioGreco/mail-cat/releases/latest), install it, open it, and add your account from the app. There is no configuration file to write.

mail-cat is **not code-signed** (no Apple or Microsoft certificate), so your system warns you the first time. The steps below get past that once; updates afterwards do not ask again.

Closing the window does not stop mail sync: a background server starts at login and keeps running. There is no tray icon.

## macOS (Apple Silicon)

1. Open `mail-cat_<version>_aarch64.dmg` and drag mail-cat to Applications.
2. Open mail-cat. macOS refuses ("Apple could not verify...").
3. Open System Settings, Privacy & Security, scroll to the Security section and click **Open Anyway** next to the mail-cat message. Confirm with your password.

On macOS 15 and later the old right-click, Open shortcut no longer works; the Privacy & Security button is the way.

## Windows

1. Run `mail-cat_<version>_x64-setup.exe`. It installs for your user only and does not ask for administrator rights.
2. If SmartScreen says "Windows protected your PC", click **More info** (Ulteriori informazioni), then **Run anyway** (Esegui comunque).

## Linux

Needs glibc 2.39 or newer: Ubuntu 24.04, Debian 13, Fedora 40 or later.

- **AppImage:** `chmod +x mail-cat_<version>_amd64.AppImage`, then run it. On first launch the bundled server is copied to `~/.local/share/mail-cat/bin/` so it can keep running after the app closes. Needs FUSE 2 on some distributions (`libfuse2`).
- **Debian / Ubuntu:** `sudo apt install ./mail-cat_<version>_amd64.deb`
- **Fedora / openSUSE:** `sudo dnf install ./mail-cat-<version>-1.x86_64.rpm`

The `.deb` and `.rpm` also put `mail-cat-server` and the `mc` command-line tool in `/usr/bin`.

## Updates

The app checks for new versions at startup and every six hours and shows a small notice: **Versione X disponibile, Aggiorna**. Nothing is installed without your confirmation. With the `.deb`/`.rpm`, an update asks for your password (polkit); if it fails, download the new package by hand.

## Where your data lives

| | Config | Database and search index |
|---|---|---|
| Linux | `~/.config/mail-cat/` | `~/.local/share/mail-cat/` |
| macOS | `~/Library/Application Support/mail-cat/` | same folder |
| Windows | `%APPDATA%\mail-cat\` | `%LOCALAPPDATA%\mail-cat\` |

Account passwords and OAuth tokens are in the system keyring (Secret Service, Keychain, Credential Manager). Server logs: `~/Library/Logs/mail-cat/` on macOS, `%LOCALAPPDATA%\mail-cat\server.log` on Windows, `journalctl --user -u mail-cat-server` on Linux.

## Uninstalling

Uninstalling removes the program and the background server's autostart, and **keeps** your data, config and keyring secrets. Delete the folders above by hand if you want them gone.

- **Windows:** Settings, Apps, mail-cat, Uninstall (the autostart task is removed for you).
- **Linux:** `sudo apt remove mail-cat` / `sudo dnf remove mail-cat`. For the AppImage, first run `~/.local/share/mail-cat/bin/mail-cat-server service uninstall`, then delete the AppImage.
- **macOS:** first run `/Applications/mail-cat.app/Contents/MacOS/mail-cat-server service uninstall`, then drag the app to the Trash. Otherwise the login item keeps pointing at a missing file.
- On Linux the autostart is a per-user systemd unit, so a package removal cannot clean it up for every user: each user can run `mail-cat-server service uninstall` before removing the package.
