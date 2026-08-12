import { PeerStorage } from "./PeerStorage.ts";
import type { FileData, PeerStorageConf } from "./types.ts";

function assertEquals<T>(actual: T, expected: T, message: string) {
    if (actual !== expected) {
        throw new Error(`${message}\nactual=${actual}\nexpected=${expected}`);
    }
}

function assert(condition: unknown, message: string) {
    if (!condition) throw new Error(message);
}

// A storage peer on a real temp directory, with the hub dispatch replaced by a stub
// we can make fail on demand. Nothing is started, so no watcher runs — we call
// dispatch() directly, the way the watcher would.
async function withPeer(
    fn: (ctx: {
        peer: PeerStorage;
        dir: string;
        file: string;
        sent: string[];
        failWith: (e: Error | undefined) => void;
    }) => Promise<void>,
) {
    const dir = await Deno.makeTempDir({ prefix: "lsb-storage-test-" });
    const config: PeerStorageConf = {
        type: "storage",
        name: `test-storage-${dir}`, // per-test settings namespace (localStorage is shared)
        baseDir: dir,
        scanOfflineChanges: true,
        useChokidar: true,
    };
    const sent: string[] = [];
    let failure: Error | undefined;
    const peer = new PeerStorage(config, (_src, path) => {
        if (failure) return Promise.reject(failure);
        sent.push(path);
        return Promise.resolve();
    });
    const file = `${dir}/note.md`;
    try {
        await fn({ peer, dir, file, sent, failWith: (e) => (failure = e) });
    } finally {
        await Deno.remove(dir, { recursive: true }).catch(() => {});
    }
}

Deno.test("a successful upload remembers the version, so the offline scan skips it", async () => {
    await withPeer(async ({ peer, file, sent }) => {
        await Deno.writeTextFile(file, "hello");

        await peer.dispatch(file);
        await new Promise((r) => setTimeout(r, 400)); // dispatch defers by 250ms

        assertEquals(sent.length, 1, "the change should have been sent once");
        assertEquals(await peer.isChanged("note.md"), false, "a sent version must count as synced");
    });
});

Deno.test("a failed upload keeps the file pending instead of marking it synced", async () => {
    // The 2026-08-12 data loss: the stat was written before the upload, so a failure
    // left the edit local-only and every later offline scan skipped it.
    await withPeer(async ({ peer, file, sent, failWith }) => {
        await Deno.writeTextFile(file, "hello");
        failWith(new Error("fetch failed"));

        await peer.dispatch(file);
        await new Promise((r) => setTimeout(r, 400));

        assertEquals(sent.length, 0, "nothing should have reached the hub");
        assertEquals(await peer.isChanged("note.md"), true, "a failed upload must stay pending for the next scan");
    });
});

Deno.test("the retry after a failed upload actually sends the file", async () => {
    await withPeer(async ({ peer, file, sent, failWith }) => {
        await Deno.writeTextFile(file, "hello");
        failWith(new Error("fetch failed"));
        await peer.dispatch(file);
        await new Promise((r) => setTimeout(r, 400));
        assertEquals(sent.length, 0, "first attempt fails");

        // CouchDB is back; this is what the offline scan does for a changed file.
        failWith(undefined);
        assert(await peer.isChanged("note.md"), "file must still be reported as changed");
        await peer.dispatch(file);
        await new Promise((r) => setTimeout(r, 400));

        assertEquals(sent.length, 1, "the retry must deliver the edit");
        assertEquals(await peer.isChanged("note.md"), false, "and then count as synced");
    });
});

Deno.test("a second watcher event must not mark a failed upload as synced", async () => {
    // The hole the 2026-08-12 blackhole test exposed: chokidar fires twice for one
    // edit. The second dispatch sees identical content ("repeating") — if that branch
    // records the version, the still-unsent edit counts as synced and is lost again.
    await withPeer(async ({ peer, file, sent, failWith }) => {
        await Deno.writeTextFile(file, "hello");
        failWith(new Error("fetch failed"));
        await peer.dispatch(file); // first event: upload fails
        await new Promise((r) => setTimeout(r, 400));

        await peer.dispatch(file); // second event for the same edit: "repeating"
        await new Promise((r) => setTimeout(r, 400));

        assertEquals(sent.length, 0, "nothing reached the hub");
        assertEquals(await peer.isChanged("note.md"), true, "the edit must still be pending after the repeat");
    });
});

Deno.test("a failed deletion does not throw out of dispatchDeleted", async () => {
    // It cannot be recovered (the file is gone), but it must not escape as an
    // unhandled rejection either — it is logged loudly instead.
    await withPeer(async ({ peer, dir, failWith }) => {
        failWith(new Error("fetch failed"));
        await peer.dispatchDeleted(`${dir}/gone.md`);
        await new Promise((r) => setTimeout(r, 400));
    });
});

Deno.test("dispatch records the version that was read, not a later edit", async () => {
    await withPeer(async ({ peer, file, sent }) => {
        await Deno.writeTextFile(file, "first");
        await peer.dispatch(file);
        await new Promise((r) => setTimeout(r, 400));
        assertEquals(sent.length, 1, "first version sent");

        // A second edit lands after the upload of the first one.
        await Deno.writeTextFile(file, "second version, longer");

        assertEquals(await peer.isChanged("note.md"), true, "the newer edit must still be seen as unsent");
    });
});

Deno.test("FileData round-trips through get() for a plain note", async () => {
    await withPeer(async ({ peer, file }) => {
        await Deno.writeTextFile(file, "content");

        const data = await peer.get("note.md");

        assert(data !== false, "get() should return data for an existing file");
        assertEquals((data as FileData).size, 7, "size should match the file");
    });
});
