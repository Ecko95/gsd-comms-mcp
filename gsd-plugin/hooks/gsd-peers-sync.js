#!/usr/bin/env node
// gsd-peers-sync — PostToolUse hook
// Automatically registers GSD subagents as claude-peers and keeps their
// summary in sync with the current task from STATE.md.
//
// How it works:
// 1. On first tool use in a session, registers with the claude-peers broker
// 2. Reads STATE.md to extract the current phase/plan/task as the summary
// 3. Updates the peer summary whenever the task changes
// 4. Unregisters when the session ends (process exit)
//
// The hook talks directly to the broker HTTP API (default localhost:7899).
// No MCP server needed — this is a lightweight sidecar.
//
// Config: .planning/config.json → hooks.peers_sync: true (default: false)

const fs = require('fs');
const path = require('path');
const os = require('os');
const http = require('http');

const BROKER_PORT = parseInt(process.env.CLAUDE_PEERS_PORT || '7899', 10);
const BROKER_URL = `http://127.0.0.1:${BROKER_PORT}`;
const STATE_FILE = path.join('.planning', 'STATE.md');
const STALE_SUMMARY_MS = 10_000; // re-read STATE.md at most every 10s

// Persistent state across hook invocations via temp file
function getStatePath(sessionId) {
  return path.join(os.tmpdir(), `gsd-peers-${sessionId}.json`);
}

function readHookState(sessionId) {
  try {
    return JSON.parse(fs.readFileSync(getStatePath(sessionId), 'utf8'));
  } catch {
    return null;
  }
}

function writeHookState(sessionId, state) {
  try {
    fs.writeFileSync(getStatePath(sessionId), JSON.stringify(state));
  } catch {
    // best effort
  }
}

// Extract current task summary from STATE.md frontmatter + content
function extractTaskSummary(cwd) {
  const statePath = path.join(cwd, STATE_FILE);
  if (!fs.existsSync(statePath)) {
    return null;
  }

  try {
    const content = fs.readFileSync(statePath, 'utf8');
    const parts = [];

    // Extract phase from frontmatter or content
    const phaseMatch = content.match(/(?:current_phase|phase)\s*:\s*(.+)/i);
    if (phaseMatch) {
      parts.push(phaseMatch[1].trim());
    }

    // Extract current plan
    const planMatch = content.match(/(?:current_plan|plan)\s*:\s*(.+)/i);
    if (planMatch) {
      parts.push(`Plan: ${planMatch[1].trim()}`);
    }

    // Extract current task
    const taskMatch = content.match(/(?:current_task|task)\s*:\s*(.+)/i);
    if (taskMatch) {
      parts.push(`Task: ${taskMatch[1].trim()}`);
    }

    if (parts.length > 0) {
      return parts.join(' | ');
    }

    // Fallback: use first heading after frontmatter
    const headingMatch = content.match(/^#+\s+(.+)$/m);
    if (headingMatch) {
      return headingMatch[1].trim();
    }

    return null;
  } catch {
    return null;
  }
}

// Simple HTTP POST to broker (no external deps)
function brokerPost(endpoint, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request(
      `${BROKER_URL}${endpoint}`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(data),
        },
        timeout: 3000,
      },
      (res) => {
        let chunks = '';
        res.on('data', (c) => (chunks += c));
        res.on('end', () => {
          try {
            resolve(JSON.parse(chunks));
          } catch {
            resolve(null);
          }
        });
      }
    );
    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('timeout'));
    });
    req.write(data);
    req.end();
  });
}

async function isBrokerAlive() {
  return new Promise((resolve) => {
    const req = http.get(`${BROKER_URL}/health`, { timeout: 2000 }, (res) => {
      resolve(res.statusCode === 200);
      res.resume();
    });
    req.on('error', () => resolve(false));
    req.on('timeout', () => {
      req.destroy();
      resolve(false);
    });
  });
}

// Get git root for cwd
function getGitRoot(cwd) {
  try {
    const { execSync } = require('child_process');
    return execSync('git rev-parse --show-toplevel', { cwd, encoding: 'utf8', stdio: ['pipe', 'pipe', 'ignore'] }).trim();
  } catch {
    return null;
  }
}

// --- Main hook logic ---

let input = '';
const stdinTimeout = setTimeout(() => process.exit(0), 10000);
process.stdin.setEncoding('utf8');
process.stdin.on('data', (chunk) => (input += chunk));
process.stdin.on('end', async () => {
  clearTimeout(stdinTimeout);
  try {
    const data = JSON.parse(input);
    const sessionId = data.session_id;
    const cwd = data.cwd || process.cwd();

    if (!sessionId) {
      process.exit(0);
    }

    // Check if peers_sync is enabled in config
    const configPath = path.join(cwd, '.planning', 'config.json');
    if (fs.existsSync(configPath)) {
      try {
        const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
        if (config.hooks?.peers_sync === false) {
          process.exit(0);
        }
      } catch {
        // ignore parse errors, default to enabled if .planning exists
      }
    } else {
      // No GSD project — exit silently
      process.exit(0);
    }

    // Check if broker is alive
    if (!(await isBrokerAlive())) {
      process.exit(0);
    }

    let state = readHookState(sessionId);

    // First invocation: register with broker
    if (!state) {
      const gitRoot = getGitRoot(cwd);
      const summary = extractTaskSummary(cwd) || `GSD executor in ${path.basename(cwd)}`;

      try {
        const reg = await brokerPost('/register', {
          pid: process.pid,
          cwd,
          git_root: gitRoot,
          tty: null,
          summary,
        });

        if (reg && reg.id) {
          state = {
            peerId: reg.id,
            lastSummary: summary,
            lastSummaryCheck: Date.now(),
          };
          writeHookState(sessionId, state);

          // Register cleanup on process exit
          const cleanupPath = path.join(os.tmpdir(), `gsd-peers-cleanup-${sessionId}.sh`);
          // Write a cleanup script that unregisters on exit
          // This is best-effort — the broker also cleans stale PIDs
          fs.writeFileSync(
            cleanupPath,
            `#!/bin/sh\ncurl -sX POST ${BROKER_URL}/unregister -H 'Content-Type: application/json' -d '{"id":"${reg.id}"}' >/dev/null 2>&1\nrm -f "${cleanupPath}" "${getStatePath(sessionId)}"\n`,
            { mode: 0o755 }
          );
        }
      } catch {
        // Broker unavailable, exit silently
        process.exit(0);
      }
    }

    // Subsequent invocations: update summary if STATE.md changed
    if (state && state.peerId) {
      const now = Date.now();
      if (now - (state.lastSummaryCheck || 0) > STALE_SUMMARY_MS) {
        const newSummary = extractTaskSummary(cwd);
        if (newSummary && newSummary !== state.lastSummary) {
          try {
            await brokerPost('/set-summary', {
              id: state.peerId,
              summary: newSummary,
            });
            state.lastSummary = newSummary;
          } catch {
            // non-critical
          }
        }
        state.lastSummaryCheck = now;
        writeHookState(sessionId, state);
      }

      // Also heartbeat to keep alive
      try {
        await brokerPost('/heartbeat', { id: state.peerId });
      } catch {
        // non-critical
      }
    }

    // No additional context to inject — exit cleanly
    process.exit(0);
  } catch {
    // Silent fail — never block tool execution
    process.exit(0);
  }
});
