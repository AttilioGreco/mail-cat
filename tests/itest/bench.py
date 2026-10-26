#!/usr/bin/env python3
"""Sync benchmark: fresh Stalwart + corpus (bench_seed.py), a byte-counting
proxy (bench_proxy.py) in front of IMAPS, then an isolated mail-cat-server
syncing alice while the SQLite DB is polled every second.

  bench.py LABEL [--root REPO] [--timeout SEC] [--no-build] [-n MESSAGES]

REPO selects the tree whose server/mc get built (target dir REPO/target) so
two versions can be compared with the same corpus seed. Results land in
.state/bench/LABEL-<timestamp>.{json,md}. Uses ports 19993/19994/50061.
"""
import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
STATE = HERE / ".state"
BENCH = STATE / "bench"
DB = STATE / "mc" / "mail-cat.db"
PROXY_PORT = 19994
BOXES = ("INBOX", "Archive", "Projects")
QUERIES = {  # per-mailbox message counts: the schema differs between main and the provider-neutral branch
    False: "SELECT b.path, count(*) FROM messages m JOIN mailboxes b ON b.id = m.mailbox_id GROUP BY 1",
    True: "SELECT b.path, count(*) FROM message_mailboxes mm JOIN mailboxes b ON b.id = mm.mailbox_id GROUP BY 1",
}


def snapshot(con, multi, proxy_json):
    counts = dict(con.execute(QUERIES[multi]).fetchall())
    states = dict(con.execute("SELECT body_state, count(*) FROM messages GROUP BY 1").fetchall())
    est = con.execute("SELECT coalesce(sum(bytes), 0) FROM downloads").fetchone()[0]
    try:
        p = json.loads(Path(proxy_json).read_text())
    except (OSError, ValueError):
        p = {}
    return dict(counts=counts, messages=sum(states.values()), pending=states.get("pending", 0),
                est=est, down=p.get("down", 0), up=p.get("up", 0), conns=p.get("conns", 0))


def open_db(deadline):
    while time.time() < deadline:
        if DB.exists():
            try:
                con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=5)
                multi = "mailbox_id" not in [r[1] for r in con.execute("PRAGMA table_info(messages)")]
                con.execute("SELECT 1 FROM downloads")
                return con, multi
            except sqlite3.Error:
                pass
        time.sleep(0.5)
    raise SystemExit("db never appeared")


def log_stats(log):
    lines = log.read_text(errors="replace").splitlines()
    pick = lambda rx: [l for l in lines if re.search(rx, l, re.I)]
    pause = pick(r"pause|idle|throttl|daily|cap\b|backoff")
    return dict(errors=len(pick(r"\bERROR\b")), warnings=len(pick(r"\bWARN\b")),
                pause_idle_lines=len(pause), pause_idle_sample=pause[:8] + pause[-4:],
                error_sample=pick(r"\bERROR\b")[:5], warn_sample=pick(r"\bWARN\b")[:5])


def gaps(csv, min_len=5):
    """Runs of >= min_len seconds with no downstream traffic between first and last traffic."""
    rows = [l.split(",") for l in csv.read_text().splitlines()[1:]]
    busy = [int(r[0]) for r in rows if int(r[1]) > 0]
    out, prev = [], None
    for t in busy:
        if prev is not None and t - prev > min_len:
            out.append((prev + 1, t - 1))
        prev = t
    return [f"{a}-{b}s ({b - a + 1}s)" for a, b in out]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("label")
    ap.add_argument("--root", default=str(HERE.parents[1]))
    ap.add_argument("--timeout", type=int, default=1800)
    ap.add_argument("--no-build", action="store_true")
    ap.add_argument("-n", type=int, default=3000)
    a = ap.parse_args()
    root = Path(a.root).resolve()
    target = Path(os.environ.get("CARGO_TARGET_DIR", root / "target")) if root == HERE.parents[1] else root / "target"
    env = {**os.environ, "CARGO_TARGET_DIR": str(target)}
    if not a.no_build:
        subprocess.run(["cargo", "build", "-q", "-p", "mc-server", "-p", "mc-cli"], cwd=root, env=env, check=True)
    ts = time.strftime("%Y%m%d-%H%M%S")
    BENCH.mkdir(parents=True, exist_ok=True)
    csv = BENCH / f"{a.label}-{ts}.timeline.csv"
    subprocess.run(["bash", str(HERE / "reset.sh"), "--stop"], check=False)
    proxy = subprocess.Popen([sys.executable, str(HERE / "bench_proxy.py"), str(PROXY_PORT), "19993", str(csv)])
    time.sleep(1)
    try:
        run = {**env, "ITEST_NO_BUILD": "1", "ITEST_IMAP_PORT": str(PROXY_PORT),
               "ITEST_BIN": str(target / "debug/mail-cat-server"), "ITEST_MC": str(target / "debug/mc"),
               "ITEST_PRESEED": f"{sys.executable} {HERE}/bench_seed.py -n {a.n}"}
        subprocess.run(["bash", str(HERE / "reset.sh")], env=run, check=True)
        result = poll(a, csv, proxy, ts)
    finally:
        subprocess.run(["bash", str(HERE / "reset.sh"), "--stop"], check=False)
        proxy.terminate()
        proxy.wait()
    return result


def poll(a, csv, proxy, ts):
    corpus = json.loads((BENCH / "corpus.json").read_text())
    total = corpus["messages"]
    inbox_total = round(a.n * 0.5)
    t0 = (STATE / "server.pid").stat().st_mtime
    deadline = t0 + a.timeout
    con, multi = open_db(deadline)
    want = dict(first_inbox=lambda s: s["counts"].get("INBOX", 0) > 0,
                inbox_complete=lambda s: s["counts"].get("INBOX", 0) >= inbox_total,
                all_headers=lambda s: s["messages"] >= total,
                bodies_complete=lambda s: s["messages"] >= total and s["pending"] == 0)
    marks, series = {}, []
    while time.time() < deadline and len(marks) < len(want):
        t = time.time() - t0
        try:
            s = snapshot(con, multi, str(csv) + ".json")
        except sqlite3.Error:
            time.sleep(1)
            continue
        series.append(dict(t=round(t, 1), **{k: v for k, v in s.items() if k != "counts"}, **s["counts"]))
        for name, ok in want.items():
            if name not in marks and ok(s):
                marks[name] = dict(t=round(t, 1), **s)
        time.sleep(1)
    time.sleep(2)  # let the proxy flush its last second
    final = snapshot(con, multi, str(csv) + ".json")
    log = BENCH / f"{a.label}-{ts}.server.log"
    log.write_text((STATE / "server.log").read_text(errors="replace"))
    res = dict(label=a.label, schema="message_mailboxes" if multi else "messages.mailbox_id", corpus=corpus,
               completed="bodies_complete" in marks, timeout_s=a.timeout, marks=marks, final=final,
               proxy=json.loads(Path(str(csv) + ".json").read_text()) if Path(str(csv) + ".json").exists() else {},
               log=log_stats(log), idle_gaps=gaps(csv), series=series)
    out = BENCH / f"{a.label}-{ts}"
    out.with_suffix(".json").write_text(json.dumps(res, indent=1))
    md = table(res)
    out.with_suffix(".md").write_text(md)
    print(md)
    return res


def table(r):
    c, m = r["corpus"], r["marks"]
    mb = lambda n: f"{n / 1e6:.2f} MB"
    rows = [("milestone", "t (s)", "wire down", "wire up", "app estimate", "conns")]
    prev = dict(down=0, up=0, est=0)
    for name in ("first_inbox", "inbox_complete", "all_headers", "bodies_complete"):
        x = m.get(name)
        rows.append((name, "-", "-", "-", "-", "-") if not x else
                    (name, f"{x['t']:.0f}", mb(x["down"]), mb(x["up"]), mb(x["est"]), str(x["conns"])))
    f, p = r["final"], r["proxy"]
    last = m.get("bodies_complete") or dict(down=f["down"], est=f["est"])
    lines = [f"### {r['label']}  ({r['schema']}; {'COMPLETE' if r['completed'] else 'TIMEOUT after %ds' % r['timeout_s']})", "",
             "| " + " | ".join(rows[0]) + " |", "|" + "---|" * len(rows[0])]
    lines += ["| " + " | ".join(x) + " |" for x in rows[1:]]
    ratio = f"{last['est'] / last['down']:.2f}" if last["down"] else "n/a"
    lines += ["",
              f"- wire total down/up: {mb(p.get('down', 0))} / {mb(p.get('up', 0))}; connections total/peak: {p.get('conns')}/{p.get('peak')}",
              f"- app-estimated (downloads) {mb(f['est'])}; estimated/wire at completion = {ratio}",
              f"- corpus {mb(c['total'])}, text {mb(c['text'])}, attachments {mb(c['attachments'])} ({c['attachment_count']}); "
              f"wire_down/text = {last['down'] / c['text']:.2f}; wire_down/corpus = {last['down'] / c['total']:.3f}",
              f"- final: {f['messages']} messages, {f['pending']} pending bodies",
              f"- log: {r['log']['errors']} ERROR, {r['log']['warnings']} WARN, {r['log']['pause_idle_lines']} pause/idle lines",
              f"- idle gaps (no downstream bytes >5s): {', '.join(r['idle_gaps'][:12]) or 'none'}"]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
