import { test, expect, beforeAll, afterAll, beforeEach } from "bun:test";

const BROKER_PORT = 17899; // Use a different port for tests
const BROKER_URL = `http://127.0.0.1:${BROKER_PORT}`;
let brokerProc: ReturnType<typeof Bun.spawn>;

async function brokerPost<T = unknown>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BROKER_URL}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return res.json() as Promise<T>;
}

beforeAll(async () => {
  // Start broker on test port with temp DB
  const dbPath = `/tmp/claude-peers-test-${Date.now()}.db`;
  brokerProc = Bun.spawn(["bun", "broker.ts"], {
    env: {
      ...process.env,
      CLAUDE_PEERS_PORT: String(BROKER_PORT),
      CLAUDE_PEERS_DB: dbPath,
    },
    stdout: "ignore",
    stderr: "pipe",
  });

  // Wait for broker to start
  for (let i = 0; i < 30; i++) {
    try {
      const res = await fetch(`${BROKER_URL}/health`, { signal: AbortSignal.timeout(500) });
      if (res.ok) return;
    } catch {}
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error("Broker failed to start");
});

afterAll(() => {
  brokerProc?.kill();
});

// --- Phase 1: Atomic transaction tests ---

test("register returns a peer ID", async () => {
  const res = await brokerPost<{ id: string }>("/register", {
    pid: process.pid,
    cwd: "/tmp/test",
    git_root: null,
    tty: null,
    summary: "test peer",
  });
  expect(res.id).toBeString();
  expect(res.id.length).toBe(8);
});

test("register re-registration cleans old peer atomically", async () => {
  // Register first time
  const first = await brokerPost<{ id: string }>("/register", {
    pid: 99990,
    cwd: "/tmp/test",
    git_root: null,
    tty: null,
    summary: "first",
  });

  // Send a message to the first peer
  const sender = await brokerPost<{ id: string }>("/register", {
    pid: 99991,
    cwd: "/tmp/test",
    git_root: null,
    tty: null,
    summary: "sender",
  });
  await brokerPost("/send-message", {
    from_id: sender.id,
    to_id: first.id,
    text: "hello",
  });

  // Re-register with same PID — should clean up old peer + messages
  const second = await brokerPost<{ id: string }>("/register", {
    pid: 99990,
    cwd: "/tmp/test",
    git_root: null,
    tty: null,
    summary: "second",
  });

  expect(second.id).not.toBe(first.id);

  // Old peer's messages should be gone, new peer should have no messages
  const poll = await brokerPost<{ messages: unknown[] }>("/poll-messages", { id: second.id });
  expect(poll.messages.length).toBe(0);
});

test("poll-messages is atomic — marks all as delivered", async () => {
  const peer = await brokerPost<{ id: string }>("/register", {
    pid: 99992,
    cwd: "/tmp/test",
    git_root: null,
    tty: null,
    summary: "receiver",
  });
  const sender = await brokerPost<{ id: string }>("/register", {
    pid: 99993,
    cwd: "/tmp/test",
    git_root: null,
    tty: null,
    summary: "sender",
  });

  // Send 3 messages
  for (let i = 0; i < 3; i++) {
    await brokerPost("/send-message", {
      from_id: sender.id,
      to_id: peer.id,
      text: `message ${i}`,
    });
  }

  // First poll should get all 3
  const first = await brokerPost<{ messages: unknown[] }>("/poll-messages", { id: peer.id });
  expect(first.messages.length).toBe(3);

  // Second poll should get 0 (all marked delivered atomically)
  const second = await brokerPost<{ messages: unknown[] }>("/poll-messages", { id: peer.id });
  expect(second.messages.length).toBe(0);
});

test("send-message to nonexistent peer returns error", async () => {
  const sender = await brokerPost<{ id: string }>("/register", {
    pid: 99994,
    cwd: "/tmp/test",
    git_root: null,
    tty: null,
    summary: "sender",
  });

  const res = await brokerPost<{ ok: boolean; error: string }>("/send-message", {
    from_id: sender.id,
    to_id: "nonexistent",
    text: "hello",
  });
  expect(res.ok).toBe(false);
  expect(res.error).toContain("not found");
});

test("unregister cleans peer + messages atomically", async () => {
  // Use the current process PID for sender so it survives alive checks
  const peer = await brokerPost<{ id: string }>("/register", {
    pid: 99995,
    cwd: "/tmp/test-unregister",
    git_root: null,
    tty: null,
    summary: "to-delete",
  });
  const sender = await brokerPost<{ id: string }>("/register", {
    pid: 99996,
    cwd: "/tmp/test-unregister",
    git_root: null,
    tty: null,
    summary: "sender",
  });

  await brokerPost("/send-message", {
    from_id: sender.id,
    to_id: peer.id,
    text: "will be cleaned",
  });

  await brokerPost("/unregister", { id: peer.id });

  // Verify unregistered peer has no pending messages by trying to poll
  const poll = await brokerPost<{ messages: unknown[] }>("/poll-messages", { id: peer.id });
  expect(poll.messages.length).toBe(0);
});

// --- Phase 2: Session tests ---

test("session-heartbeat creates peer + session atomically", async () => {
  const res = await brokerPost<{ peer_id: string; session_id: string }>("/session-heartbeat", {
    session_id: "test-session-001",
    pid: process.pid,
    cwd: "/tmp/test-session",
    git_root: "/tmp/test-repo",
    task_summary: "Working on feature X",
  });

  expect(res.peer_id).toBeString();
  expect(res.session_id).toBe("test-session-001");

  // Session should be queryable
  const status = await brokerPost<{ session_id: string; task_summary: string; status: string }>(
    "/session-status",
    { session_id: "test-session-001" }
  );
  expect(status.session_id).toBe("test-session-001");
  expect(status.task_summary).toBe("Working on feature X");
  expect(status.status).toBe("active");
});

test("session-heartbeat is idempotent — second call updates, doesn't duplicate", async () => {
  const first = await brokerPost<{ peer_id: string }>("/session-heartbeat", {
    session_id: "test-session-idem",
    pid: process.pid,
    cwd: "/tmp/test",
    git_root: null,
    task_summary: "First summary",
  });

  const second = await brokerPost<{ peer_id: string }>("/session-heartbeat", {
    session_id: "test-session-idem",
    pid: process.pid,
    cwd: "/tmp/test",
    git_root: null,
    task_summary: "Updated summary",
  });

  // Same peer ID
  expect(second.peer_id).toBe(first.peer_id);

  // Summary should be updated
  const status = await brokerPost<{ task_summary: string }>("/session-status", {
    session_id: "test-session-idem",
  });
  expect(status.task_summary).toBe("Updated summary");
});

test("session-end cleans session + peer atomically", async () => {
  const res = await brokerPost<{ peer_id: string }>("/session-heartbeat", {
    session_id: "test-session-end",
    pid: 99997,
    cwd: "/tmp/test",
    git_root: null,
    task_summary: "About to end",
  });

  await brokerPost("/session-end", { session_id: "test-session-end" });

  const status = await brokerPost<{ status?: string; error?: string }>("/session-status", {
    session_id: "test-session-end",
  });
  // Session is completed (not deleted, for audit)
  expect(status.status).toBe("completed");
});

// --- Phase 3: Wave / orchestration tests ---

test("wave-create creates wave + tasks atomically", async () => {
  const res = await brokerPost<{ wave_id: number; task_ids: number[] }>("/wave-create", {
    repo: "/tmp/test-repo",
    phase: 1,
    wave_number: 1,
    tasks: [
      { name: "T01: Build auth module", files: ["src/auth.ts", "src/auth.test.ts"] },
      { name: "T02: Build user model", files: ["src/user.ts"] },
      { name: "T03: Build API routes", files: ["src/routes.ts"] },
    ],
  });

  expect(res.wave_id).toBeNumber();
  expect(res.task_ids.length).toBe(3);

  // Wave status should show all tasks
  const status = await brokerPost<{ wave: { status: string }; tasks: { task_name: string; status: string }[] }>(
    "/wave-status",
    { wave_id: res.wave_id }
  );
  expect(status.wave.status).toBe("pending");
  expect(status.tasks.length).toBe(3);
  expect(status.tasks[0].status).toBe("pending");
});

test("wave-create is idempotent", async () => {
  const first = await brokerPost<{ wave_id: number }>("/wave-create", {
    repo: "/tmp/test-repo",
    phase: 1,
    wave_number: 2,
    tasks: [{ name: "T01", files: [] }],
  });

  const second = await brokerPost<{ wave_id: number }>("/wave-create", {
    repo: "/tmp/test-repo",
    phase: 1,
    wave_number: 2,
    tasks: [{ name: "T01", files: [] }],
  });

  expect(second.wave_id).toBe(first.wave_id);
});

test("task-start assigns session + detects file conflicts", async () => {
  const wave = await brokerPost<{ wave_id: number; task_ids: number[] }>("/wave-create", {
    repo: "/tmp/conflict-test",
    phase: 1,
    wave_number: 1,
    tasks: [
      { name: "T01", files: ["shared.ts", "a.ts"] },
      { name: "T02", files: ["shared.ts", "b.ts"] },
      { name: "T03", files: ["c.ts"] },
    ],
  });

  // Create sessions (different PIDs to avoid peer clobbering)
  const s1 = await brokerPost<{ peer_id: string }>("/session-heartbeat", {
    session_id: "conflict-s1",
    pid: 77701,
    cwd: "/tmp/test",
    git_root: null,
    task_summary: "worker 1",
  });
  const s2 = await brokerPost<{ peer_id: string }>("/session-heartbeat", {
    session_id: "conflict-s2",
    pid: 77702,
    cwd: "/tmp/test",
    git_root: null,
    task_summary: "worker 2",
  });

  // Start T01
  const start1 = await brokerPost<{ ok: boolean }>("/task-start", {
    task_id: wave.task_ids[0],
    session_id: "conflict-s1",
  });
  expect(start1.ok).toBe(true);

  // Start T02 — should fail due to shared.ts conflict
  const start2 = await brokerPost<{ ok: boolean; error: string }>("/task-start", {
    task_id: wave.task_ids[1],
    session_id: "conflict-s2",
  });
  expect(start2.ok).toBe(false);
  expect(start2.error).toContain("conflict");
  expect(start2.error).toContain("shared.ts");

  // Start T03 — no conflict, should succeed
  const start3 = await brokerPost<{ ok: boolean }>("/task-start", {
    task_id: wave.task_ids[2],
    session_id: "conflict-s2",
  });
  expect(start3.ok).toBe(true);
});

test("task-complete auto-completes wave when all tasks done", async () => {
  const wave = await brokerPost<{ wave_id: number; task_ids: number[] }>("/wave-create", {
    repo: "/tmp/auto-complete-test",
    phase: 1,
    wave_number: 1,
    tasks: [
      { name: "T01", files: [] },
      { name: "T02", files: [] },
    ],
  });

  // Create session + start both tasks
  await brokerPost("/session-heartbeat", {
    session_id: "auto-s1",
    pid: 77704,
    cwd: "/tmp/test",
    git_root: null,
    task_summary: "w1",
  });
  await brokerPost("/task-start", { task_id: wave.task_ids[0], session_id: "auto-s1" });
  await brokerPost("/task-start", { task_id: wave.task_ids[1], session_id: "auto-s1" });

  // Complete T01
  const r1 = await brokerPost<{ ok: boolean; wave_completed: boolean }>("/task-complete", {
    task_id: wave.task_ids[0],
  });
  expect(r1.ok).toBe(true);
  expect(r1.wave_completed).toBe(false);

  // Complete T02 — wave should auto-complete
  const r2 = await brokerPost<{ ok: boolean; wave_completed: boolean }>("/task-complete", {
    task_id: wave.task_ids[1],
  });
  expect(r2.ok).toBe(true);
  expect(r2.wave_completed).toBe(true);

  // Verify wave status
  const status = await brokerPost<{ wave: { status: string } }>("/wave-status", { wave_id: wave.wave_id });
  expect(status.wave.status).toBe("completed");
});

test("conflict-check finds overlapping files", async () => {
  const wave = await brokerPost<{ wave_id: number; task_ids: number[] }>("/wave-create", {
    repo: "/tmp/conflict-check-test",
    phase: 1,
    wave_number: 1,
    tasks: [
      { name: "T01", files: ["src/a.ts", "src/shared.ts"] },
    ],
  });

  await brokerPost("/session-heartbeat", {
    session_id: "cc-s1",
    pid: 77703,
    cwd: "/tmp/test",
    git_root: null,
    task_summary: "w1",
  });
  await brokerPost("/task-start", { task_id: wave.task_ids[0], session_id: "cc-s1" });

  const check = await brokerPost<{ conflicts: { conflicting_files: string[] }[] }>("/conflict-check", {
    wave_id: wave.wave_id,
    files: ["src/shared.ts", "src/b.ts"],
  });

  expect(check.conflicts.length).toBe(1);
  expect(check.conflicts[0].conflicting_files).toEqual(["src/shared.ts"]);
});

// --- Phase 4: Structured messages ---

test("send-message supports msg_type and payload", async () => {
  const p1 = await brokerPost<{ id: string }>("/register", {
    pid: 99998,
    cwd: "/tmp/test",
    git_root: null,
    tty: null,
    summary: "p1",
  });
  const p2 = await brokerPost<{ id: string }>("/register", {
    pid: 99999,
    cwd: "/tmp/test",
    git_root: null,
    tty: null,
    summary: "p2",
  });

  await brokerPost("/send-message", {
    from_id: p1.id,
    to_id: p2.id,
    text: "Task T01 completed",
    msg_type: "task_complete",
    payload: { task_id: 42, wave_id: 1 },
  });

  const poll = await brokerPost<{ messages: { text: string; msg_type: string; payload: string }[] }>(
    "/poll-messages",
    { id: p2.id }
  );

  expect(poll.messages.length).toBe(1);
  expect(poll.messages[0].msg_type).toBe("task_complete");
  expect(JSON.parse(poll.messages[0].payload)).toEqual({ task_id: 42, wave_id: 1 });
});

test("ack-message marks specific messages as delivered", async () => {
  const p1 = await brokerPost<{ id: string }>("/register", {
    pid: 88881,
    cwd: "/tmp/test",
    git_root: null,
    tty: null,
    summary: "ack-sender",
  });
  const p2 = await brokerPost<{ id: string }>("/register", {
    pid: 88882,
    cwd: "/tmp/test",
    git_root: null,
    tty: null,
    summary: "ack-receiver",
  });

  await brokerPost("/send-message", { from_id: p1.id, to_id: p2.id, text: "msg1" });
  await brokerPost("/send-message", { from_id: p1.id, to_id: p2.id, text: "msg2" });

  // Poll gets both (and marks them delivered already via the atomic txn)
  const poll = await brokerPost<{ messages: { id: number }[] }>("/poll-messages", { id: p2.id });
  expect(poll.messages.length).toBe(2);

  // ACK is also available as an explicit endpoint
  const ack = await brokerPost<{ ok: boolean }>("/ack-message", {
    message_ids: poll.messages.map((m) => m.id),
  });
  expect(ack.ok).toBe(true);

  // No more messages
  const poll2 = await brokerPost<{ messages: unknown[] }>("/poll-messages", { id: p2.id });
  expect(poll2.messages.length).toBe(0);
});
