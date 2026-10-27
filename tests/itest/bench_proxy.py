#!/usr/bin/env python3
"""Byte-counting TCP pass-through (TLS untouched) in front of Stalwart IMAPS.

  bench_proxy.py LISTEN_PORT [UPSTREAM_PORT] CSV_PATH [--latency-ms N]

--latency-ms delays every forwarded chunk by N ms per direction (order kept),
so a request/response round trip costs ~2N. Counts bytes per direction (down = server->client), connections, peak
concurrency; writes a per-second timeline CSV (t,down,up,conns) and a final
stats JSON next to it (<csv>.json). SIGTERM/SIGINT flush and exit.
"""
import asyncio
import json
import signal
import sys
import time

args = sys.argv[1:]
latency = 0.0
if "--latency-ms" in args:
    i = args.index("--latency-ms")
    latency = int(args[i + 1]) / 1000
    del args[i : i + 2]
listen, upstream, csv_path = int(args[0]), int(args[1]) if len(args) > 2 else 19993, args[-1]
st = dict(down=0, up=0, conns=0, open=0, peak=0, t0=None)
timeline = {}  # second -> [down, up]


def bump(direction, n):
    now = time.time()
    st["t0"] = st["t0"] or now
    sec = int(now - st["t0"])
    row = timeline.setdefault(sec, [0, 0, 0])
    row[direction] += n
    row[2] = st["open"]
    st["down" if direction == 0 else "up"] += n


async def pipe(r, w, direction):
    # Chunks queue with a release time, so delay never reorders and never serialises.
    q = asyncio.Queue()

    async def deliver():
        while (item := await q.get()) is not None:
            release, data = item
            if (wait := release - time.monotonic()) > 0:
                await asyncio.sleep(wait)
            w.write(data)
            await w.drain()

    sender = asyncio.create_task(deliver())
    try:
        while data := await r.read(65536):
            bump(direction, len(data))
            q.put_nowait((time.monotonic() + latency, data))
        q.put_nowait(None)
        await sender
    except (ConnectionError, asyncio.CancelledError):
        sender.cancel()
    finally:
        w.close()


async def handle(cr, cw):
    st["conns"] += 1
    st["open"] += 1
    st["peak"] = max(st["peak"], st["open"])
    try:
        ur, uw = await asyncio.open_connection("127.0.0.1", upstream)
        await asyncio.gather(pipe(ur, cw, 0), pipe(cr, uw, 1))
    except OSError:
        cw.close()
    finally:
        st["open"] -= 1


def dump():
    with open(csv_path, "w") as f:
        f.write("t,down,up,conns\n")
        for s in range(max(timeline, default=-1) + 1):
            d, u, c = timeline.get(s, [0, 0, st["open"]])
            f.write(f"{s},{d},{u},{c}\n")
    json.dump({**st, "t0": st["t0"]}, open(csv_path + ".json", "w"))


async def main():
    srv = await asyncio.start_server(handle, "127.0.0.1", listen)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    async with srv:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), 1)
            except asyncio.TimeoutError:
                dump()  # live: the driver reads the json for milestone byte counts
    dump()


asyncio.run(main())
