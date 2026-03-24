# GSD Plugin for claude-peers

Integrates [claude-peers](https://github.com/ecko95/claude-peers-mcp) with [GSD v1](https://github.com/gsd-build/get-shit-done) so that GSD subagents automatically register as peers and can coordinate in real time.

## What It Does

- **Auto-registration**: Each GSD executor registers with the claude-peers broker on first tool use
- **Summary sync**: Reads STATE.md and keeps the peer summary updated with the current phase/plan/task
- **Heartbeat**: Keeps the peer alive in the broker while the session is active
- **Conflict detection**: The peer coordinator agent checks for file-level conflicts before wave execution
- **Cross-agent messaging**: Executors can message each other about blockers and dependency completion

## Setup

### 1. Install the PostToolUse Hook

Add to your Claude Code settings (`.claude/settings.json` or global settings):

```json
{
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "command",
            "command": "node /path/to/claude-peers-mcp/gsd-plugin/hooks/gsd-peers-sync.js"
          }
        ]
      }
    ]
  }
}
```

Replace `/path/to/claude-peers-mcp` with the actual path to this repository.

### 2. Enable in GSD Config

Add to your project's `.planning/config.json`:

```json
{
  "hooks": {
    "peers_sync": true
  }
}
```

### 3. Add Peer Instructions to CLAUDE.md

Append the contents of `CLAUDE.md.snippet` to your GSD project's `CLAUDE.md`:

```bash
cat /path/to/claude-peers-mcp/gsd-plugin/CLAUDE.md.snippet >> your-project/CLAUDE.md
```

### 4. (Optional) Copy the Peer Coordinator Agent

If you want the orchestrator to be able to spawn a peer coordinator:

```bash
cp /path/to/claude-peers-mcp/gsd-plugin/agents/gsd-peer-coordinator.md \
   your-project/.claude/agents/gsd-peer-coordinator.md
```

## Requirements

- The claude-peers broker must be running (auto-started by the MCP server, or run `bun broker.ts` manually)
- Node.js or Bun available (the hook uses plain Node.js — no Bun-specific APIs — for compatibility)
- GSD v1 installed and active in the project (`.planning/config.json` must exist)

## How It Works

```
GSD Orchestrator
  │
  ├─ spawns Executor A ──► gsd-peers-sync hook ──► broker /register
  │                         (PostToolUse)           broker /set-summary (from STATE.md)
  │                                                 broker /heartbeat (ongoing)
  │
  ├─ spawns Executor B ──► same flow
  │
  └─ spawns Peer Coordinator ──► broker /list-peers
                                  broker /send-message (if requested)
```

The hook communicates directly with the broker's HTTP API — no MCP server needed. This keeps it lightweight and compatible with any GSD runtime (Claude Code, Gemini CLI, etc.).

## Configuration Reference

| Setting | Location | Default | Description |
|---|---|---|---|
| `hooks.peers_sync` | `.planning/config.json` | `true` | Enable/disable the peers sync hook |
| `CLAUDE_PEERS_PORT` | Environment | `7899` | Broker port override |
