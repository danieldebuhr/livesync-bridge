import { PeerCouchDB } from "./PeerCouchDB.ts";
import type { PeerCouchDBConf } from "./types.ts";

function assertEquals<T>(actual: T, expected: T, message: string) {
    if (actual !== expected) {
        throw new Error(`${message}\nactual=${actual}\nexpected=${expected}`);
    }
}

function assert(condition: unknown, message: string) {
    if (!condition) throw new Error(message);
}

// A peer that never touches CouchDB: the constructor builds no manipulator, so we
// can drive health() purely through the state the watch would have produced.
// `watching` lives in the library's manipulator, hence the stub.
function makePeer(state: { connected?: boolean; watching?: boolean; remoteEmpty?: boolean; feedSeenAt?: number }) {
    const config: PeerCouchDBConf = {
        type: "couchdb",
        name: "test-couchdb",
        baseDir: "",
        database: "test",
        url: "http://127.0.0.1:5984",
        username: "test",
        password: "test",
        passphrase: "",
        obfuscatePassphrase: "",
    };
    const peer = new PeerCouchDB(config, async () => {});
    const p = peer as unknown as Record<string, unknown>;
    p["_connected"] = state.connected ?? true;
    p["_remoteEmpty"] = state.remoteEmpty ?? false;
    p["_feedSeenAt"] = state.feedSeenAt ?? 0;
    p["man"] = { watching: state.watching ?? true };
    return peer;
}

const STALL_MS = 240_000;

Deno.test("health reports a live watch as ok", () => {
    const health = makePeer({ feedSeenAt: Date.now() - 5_000 }).health();

    assert(health.ok, "a feed with recent traffic must be ok");
    assertEquals(health.detail, "watching", "detail should be the plain watching state");
});

Deno.test("health reports a watch without feed traffic as stalled, not ok", () => {
    // The failure this guards: our idle abort can end the feed without the library's
    // complete/error handlers firing, so `watching` stays true and no reconnect ever
    // happens. Pushes keep working, so only the missing feed traffic reveals it.
    const health = makePeer({ feedSeenAt: Date.now() - (STALL_MS + 10_000) }).health();

    assert(!health.ok, "a watch with no feed traffic past the stall window must not be ok");
    assert(health.detail?.startsWith("watch stalled"), `detail should name the stall, got: ${health.detail}`);
});

Deno.test("health does not call a just-started watch stalled", () => {
    // _feedSeenAt === 0: the feed has not run yet (still connecting, or an empty
    // remote). Judging that as stalled would restart a bridge that is merely young.
    const health = makePeer({ feedSeenAt: 0 }).health();

    assert(health.ok, "no feed history yet must not read as stalled");
    assertEquals(health.detail, "watching", "detail should be the plain watching state");
});

Deno.test("health ignores feed staleness when not watching", () => {
    const health = makePeer({ watching: false, remoteEmpty: true, feedSeenAt: Date.now() - (STALL_MS + 10_000) }).health();

    assert(health.ok, "an empty remote has no feed to stall");
    assertEquals(health.detail, "connected (empty remote)", "detail should report the empty remote");
});

Deno.test("probeHealth turns a stall into restartWorthy once the backend is up", async () => {
    const peer = makePeer({ feedSeenAt: Date.now() - (STALL_MS + 10_000) });
    const p = peer as unknown as Record<string, unknown>;
    // probeHealth only judges a peer that was healthy before, and only after the
    // grace window — mimic both, so this test covers the path the watchdog uses.
    p["_everOk"] = true;
    p["_notOkSince"] = Date.now() - 120_000;
    p["checkBackendUp"] = () => Promise.resolve(true);

    const health = await peer.probeHealth();

    assert(health.restartWorthy, "a stalled watch with CouchDB reachable must be restart-worthy");
});

Deno.test("probeHealth keeps a stall non-restart-worthy while CouchDB is unreachable", async () => {
    const peer = makePeer({ feedSeenAt: Date.now() - (STALL_MS + 10_000) });
    const p = peer as unknown as Record<string, unknown>;
    p["_everOk"] = true;
    p["_notOkSince"] = Date.now() - 120_000;
    p["checkBackendUp"] = () => Promise.resolve(false);

    const health = await peer.probeHealth();

    assert(!health.restartWorthy, "restarting cannot fix an unreachable CouchDB — no churn during an outage");
});

Deno.test("probeHealth restarts the grace window when CouchDB comes back", async () => {
    // A VPN drop (nightly forced reconnect on the home line) makes the peer stall
    // for minutes with the backend down. The grace window must not have expired
    // during that time, or the first probe after the tunnel returns restarts the
    // service before the watch's own ~10s reconnect can heal it.
    const peer = makePeer({ feedSeenAt: Date.now() - (STALL_MS + 600_000) });
    const p = peer as unknown as Record<string, unknown>;
    p["_everOk"] = true;
    let backendUp = false;
    p["checkBackendUp"] = () => Promise.resolve(backendUp);

    await peer.probeHealth(); // outage: stalled, backend down
    backendUp = true;
    const first = await peer.probeHealth(); // tunnel just came back

    assert(!first.restartWorthy, "the grace window must start when the backend returns, not during the outage");
    assert(first.backendUp, "backendUp should reflect the reachable CouchDB");
});

Deno.test("only a delivered byte counts as feed liveness, not the sent request", async () => {
    // The hole a blocked-tunnel test exposed: liveness used to be stamped when the
    // request went out, so a reconnect loop that never gets an answer kept health
    // green forever. Only bytes coming back may refresh it.
    const peer = makePeer({});
    const p = peer as unknown as Record<string, unknown>;
    const stale = Date.now() - 999_999;
    p["_feedSeenAt"] = stale;

    let ctrl!: ReadableStreamDefaultController<Uint8Array>;
    const body = new ReadableStream<Uint8Array>({ start: (c) => { ctrl = c; } });
    const origFetch = globalThis.fetch;
    globalThis.fetch = () => Promise.resolve(new Response(body));
    try {
        const res = await (peer as unknown as {
            _fetchWithIdleTimeout: (u: string) => Promise<Response>;
        })._fetchWithIdleTimeout("https://couch.example/db/_changes?feed=continuous");
        assertEquals(p["_feedSeenAt"] as number, stale, "sending the request must not refresh liveness");

        const reader = res.body!.getReader();
        ctrl.enqueue(new TextEncoder().encode("\n")); // CouchDB heartbeat
        await reader.read();
        assert((p["_feedSeenAt"] as number) > stale, "a delivered byte must refresh liveness");

        ctrl.close();
        await reader.read(); // drain to completion so the idle timer is cleared
    } finally {
        globalThis.fetch = origFetch;
    }
});
