## Entire Metadata System

Entire stores all its metadata **inside the git repository itself**, using orphan branches and shadow branches that never touch the user's working branch. There are three categories of git refs:

### 1. Shadow Branches (Temporary Checkpoints)

**Ref pattern:** `entire/<commit-hash[:7]>-<worktreeHash[:6]>`

**Purpose:** Intra-session save points. These capture the full working tree state (code + session metadata) as the agent works, before the user has committed anything. Used for **rewind** — restoring to any point mid-session.

**When captured:** Every time the agent modifies files during a session (on lifecycle hooks like file-change detection). Deduplicated — if the tree hasn't changed since the last checkpoint, no new commit is created.

**Format:** Each commit on a shadow branch contains:
- The actual working tree files (code the agent wrote/modified)
- `.entire/metadata/<session-id>/full.jsonl` — session transcript
- `.entire/metadata/<session-id>/prompt.txt` — user prompts
- `.entire/metadata/<session-id>/tasks/` — subagent task data (if any)
- Commit trailers: `Entire-Session`, `Entire-Metadata`, `Entire-Strategy`

**Lifecycle:** Temporary. When the user commits, the session data is "condensed" (copied) to the permanent metadata branch, and the shadow branch can be cleaned up. If the user rebases or pulls (changing HEAD), the shadow branch is automatically migrated to the new base commit hash.

---

### 2. `entire/checkpoints/v1` (Committed Checkpoints)

**Ref:** `entire/checkpoints/v1` (orphan branch)

**Purpose:** The **permanent record** of AI-assisted work. Answers "how/when" — what the agent did, what files it touched, token usage, attribution metrics, and the full session transcript. Linked to user commits via the `Entire-Checkpoint` trailer.

**When captured:** At commit time. When the user runs `git commit`, the `prepare-commit-msg` hook generates a checkpoint ID, and the `post-commit` hook condenses the shadow branch data onto this branch. Also updated at session stop with the final complete transcript.

**Format:** Sharded directory tree, one directory per checkpoint:

```
<checkpoint-id[:2]>/<checkpoint-id[2:]>/
├── metadata.json              # CheckpointSummary (aggregated stats, file list, token usage)
├── 0/                         # Session slot (0-based, supports multiple sessions per checkpoint)
│   ├── metadata.json          # Per-session: agent, model, turn ID, attribution, summary
│   ├── full.jsonl             # Session transcript (agent conversation log)
│   ├── prompt.txt             # User prompts for this session
│   ├── content_hash.txt       # SHA256 of transcript (dedup/integrity)
│   └── tasks/<tool-use-id>/   # Subagent task checkpoints
│       ├── checkpoint.json
│       └── agent-<id>.jsonl
├── 1/                         # Additional sessions (if multiple sessions before commit)
└── ...
```

Each commit on this branch has the subject `Checkpoint: <id>` and trailers like `Entire-Session`, `Entire-Strategy`, `Entire-Agent`.

**Key metadata fields:** checkpoint ID, session ID, strategy, branch, files touched, agent type, model, token usage, AI-generated summary (intent/outcome/learnings/friction/open items), and initial attribution (agent lines vs human lines).

---

### 3. `entire/trails/v1` (Trails)

**Ref:** `entire/trails/v1` (orphan branch)

**Purpose:** Branch-centric **work tracking**. Answers "why/what" — human intent, like a lightweight git-native PR/issue. While checkpoints are machine snapshots tied to commits, trails are tied to **branches** and track the higher-level narrative: what you're working on, why, who's reviewing it, and what status it's in.

**When captured:** Created when a trail is explicitly opened for a branch. Updated as status changes (draft → open → in_progress → in_review → merged/closed). Checkpoint references are appended as commits happen.

**Format:** Same sharding scheme as checkpoints:

```
<trail-id[:2]>/<trail-id[2:]>/
├── metadata.json       # Branch, base, title, body, author, status, priority, type,
│                       # assignees, labels, reviewers (with approval status)
├── discussion.json     # Comments with replies (resolvable threads)
└── checkpoints.json    # Ordered list of checkpoint references (newest first),
                        # each linking a checkpoint ID + commit SHA + timestamp
```

**Statuses:** draft, open, in_progress, in_review, merged, closed
**Types:** bug, feature, chore, docs, refactor

---

### How They Relate

```
Trail (why/what)                    Checkpoint (how/when)
entire/trails/v1                    entire/checkpoints/v1
  │                                   ▲
  │ checkpoints.json                  │ Entire-Checkpoint trailer
  │ links trail → checkpoint IDs      │ links user commit → checkpoint
  └──────────────────────────────────►│
                                      │
                                Shadow branches (entire/<hash>-<wt>)
                                  temporary working state
                                  condensed → checkpoints/v1 on commit
```

A **trail** tracks a branch's lifecycle and purpose. As you work, **shadow branches** capture incremental snapshots. When you commit, those snapshots are condensed into a **committed checkpoint**, and a reference is added to the trail's checkpoint list. The user's commit gets an `Entire-Checkpoint` trailer linking back to the metadata.
