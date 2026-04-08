use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use notify::{Config, RecommendedWatcher, RecursiveMode, Watcher};
use time::OffsetDateTime;

use crate::context::CommandContext;
use gmeta::types::{Target, TargetType, ValueType};
use gmeta::Session;

// ANSI colors
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const MAGENTA: &str = "\x1b[35m";
const CYAN: &str = "\x1b[36m";
const RED: &str = "\x1b[31m";
const RESET: &str = "\x1b[0m";

pub fn run(agent: &str, debounce_secs: u64) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.session.repo();
    let workdir = repo
        .workdir()
        .context("bare repo not supported")?
        .to_path_buf();
    let workdir = workdir.canonicalize()?;

    let transcripts_dir = resolve_transcripts_dir(agent, &workdir)?;
    let git_dir = repo.path().to_path_buf();

    eprintln!(
        "{}{}[watch]{} Watching {} transcripts: {}",
        BOLD,
        CYAN,
        RESET,
        agent,
        transcripts_dir.display()
    );
    eprintln!(
        "{}{}[watch]{} Watching git refs: {}",
        BOLD,
        CYAN,
        RESET,
        git_dir.join("refs").display()
    );
    eprintln!("{BOLD}{CYAN}[watch]{RESET} Debounce: {debounce_secs}s -- press Ctrl+C to stop\n");

    // Set up file watcher
    let (tx, rx) = mpsc::channel();
    let tx2 = tx.clone();
    let mut watcher = RecommendedWatcher::new(
        move |res| {
            let _ = tx2.send(res);
        },
        Config::default(),
    )
    .context("failed to create file watcher")?;

    // Watch transcripts directory
    if transcripts_dir.exists() {
        watcher.watch(&transcripts_dir, RecursiveMode::Recursive)?;
    } else {
        eprintln!("  {YELLOW}[warn]{RESET} Transcripts dir does not exist yet, watching parent...");
        // Watch parent so we catch when the directory is created
        if let Some(parent) = transcripts_dir.parent() {
            if parent.exists() {
                watcher.watch(parent, RecursiveMode::NonRecursive)?;
            }
        }
    }

    // Watch .git/HEAD and refs for commit detection
    let head_path = git_dir.join("HEAD");
    if head_path.exists() {
        watcher.watch(&head_path, RecursiveMode::NonRecursive)?;
    }
    let refs_dir = git_dir.join("refs");
    if refs_dir.exists() {
        watcher.watch(&refs_dir, RecursiveMode::Recursive)?;
    }

    // Initialize state
    let mut state = WatchState::new(&workdir, &transcripts_dir)?;

    // Scan existing transcript files so we only process new content
    state.init_file_positions()?;

    // Take initial snapshot of gitbutler status
    state.refresh_gitbutler_status()?;

    // Event loop
    loop {
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(Ok(event)) => {
                for path in &event.paths {
                    if path.extension().is_some_and(|e| e == "jsonl")
                        && path.starts_with(&transcripts_dir)
                    {
                        state.handle_transcript_change(path)?;
                    } else if path.starts_with(&git_dir) {
                        state.mark_git_dirty();
                    }
                }
            }
            Ok(Err(e)) => {
                eprintln!("{RED}[error]{RESET} Watch error: {e}");
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                state.tick(debounce_secs)?;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    Ok(())
}

fn resolve_transcripts_dir(agent: &str, workdir: &Path) -> Result<PathBuf> {
    match agent {
        "claude" => {
            let home = std::env::var("HOME").context("HOME not set")?;
            // Claude uses the absolute path with / replaced by -
            let dir_name = workdir.to_string_lossy().replace('/', "-");
            Ok(PathBuf::from(format!("{home}/.claude/projects/{dir_name}")))
        }
        _ => bail!("unsupported agent: {agent} (supported: claude)"),
    }
}

struct WatchState {
    workdir: PathBuf,
    transcripts_dir: PathBuf,
    // Transcript tracking
    file_positions: HashMap<PathBuf, u64>,
    session_lines: HashMap<String, Vec<String>>,
    last_transcript_activity: Option<Instant>,
    active_session_id: Option<String>,
    agent_idle: bool,
    prompts_attached_up_to: HashMap<String, usize>, // session_id -> index into session_lines
    // Git tracking
    known_commits: BTreeSet<String>,
    branch_for_commit: BTreeMap<String, String>, // commit_id -> branch_name
    branch_first_seen: HashMap<String, i64>,     // branch_name -> first-seen epoch ms
    last_committed_branch: Option<String>,
    git_dirty: bool,
    last_git_event: Option<Instant>,
}

impl WatchState {
    fn new(workdir: &Path, transcripts_dir: &Path) -> Result<Self> {
        Ok(Self {
            workdir: workdir.to_path_buf(),
            transcripts_dir: transcripts_dir.to_path_buf(),
            file_positions: HashMap::new(),
            session_lines: HashMap::new(),
            last_transcript_activity: None,
            active_session_id: None,
            agent_idle: true,
            prompts_attached_up_to: HashMap::new(),
            known_commits: BTreeSet::new(),
            branch_for_commit: BTreeMap::new(),
            branch_first_seen: HashMap::new(),
            last_committed_branch: None,
            git_dirty: false,
            last_git_event: None,
        })
    }

    fn init_file_positions(&mut self) -> Result<()> {
        if !self.transcripts_dir.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(&self.transcripts_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "jsonl") && path.is_file() {
                let size = entry.metadata()?.len();
                self.file_positions.insert(path, size);
            }
        }
        if !self.file_positions.is_empty() {
            eprintln!(
                "  {}[init]{} Tracking {} existing transcript files",
                DIM,
                RESET,
                self.file_positions.len()
            );
        }
        Ok(())
    }

    fn handle_transcript_change(&mut self, path: &Path) -> Result<()> {
        let last_pos = self.file_positions.get(path).copied().unwrap_or(0);

        let Ok(file) = std::fs::File::open(path) else {
            return Ok(()); // File might have been deleted
        };
        let file_size = file.metadata()?.len();

        if file_size <= last_pos {
            return Ok(());
        }

        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(last_pos))?;

        let mut new_lines = Vec::new();
        let mut buf = String::new();
        while reader.read_line(&mut buf)? > 0 {
            let line = buf.trim().to_string();
            if !line.is_empty() {
                new_lines.push(line);
            }
            buf.clear();
        }

        self.file_positions.insert(path.to_path_buf(), file_size);

        for line in &new_lines {
            self.process_transcript_line(line);
        }

        if !new_lines.is_empty() {
            self.last_transcript_activity = Some(Instant::now());
            self.agent_idle = false;
        }

        Ok(())
    }

    fn process_transcript_line(&mut self, line: &str) {
        let parsed: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => return,
        };

        let msg_type = parsed["type"].as_str().unwrap_or("");
        let session_id = parsed["sessionId"].as_str().unwrap_or("").to_string();

        if !session_id.is_empty() {
            // Detect new session
            if self.active_session_id.as_deref() != Some(&session_id) {
                let short_id = &session_id[..8.min(session_id.len())];
                let ts = parsed["timestamp"].as_str().unwrap_or("");
                eprintln!("\n{BOLD}{YELLOW}[session]{RESET} {short_id} {DIM}{ts}{RESET}");
                self.active_session_id = Some(session_id.clone());
            }
            self.session_lines
                .entry(session_id)
                .or_default()
                .push(line.to_string());
        }

        match msg_type {
            "assistant" => {
                if let Some(content) = parsed["message"]["content"].as_array() {
                    for block in content {
                        match block["type"].as_str() {
                            Some("text") => {
                                if let Some(text) = block["text"].as_str() {
                                    let text = text.trim();
                                    if text.is_empty() {
                                        continue;
                                    }
                                    let preview = truncate(text, 100).replace('\n', " ");
                                    eprintln!("  {DIM}[text]{RESET} {preview}");
                                }
                            }
                            Some("tool_use") => {
                                // Skip tool calls -- too noisy
                            }
                            _ => {}
                        }
                    }
                }
            }
            "user" => {
                let is_meta = parsed["isMeta"].as_bool().unwrap_or(false);
                if !is_meta {
                    for text in extract_content_texts(&parsed["message"]["content"]) {
                        let preview = truncate(&text, 100).replace('\n', " ");
                        eprintln!("  {BOLD}{MAGENTA}[user]{RESET} {preview}");
                    }
                }
            }
            "last-prompt" => {
                eprintln!("  {DIM}[turn complete]{RESET}");
            }
            _ => {}
        }
    }

    fn mark_git_dirty(&mut self) {
        self.git_dirty = true;
        self.last_git_event = Some(Instant::now());
    }

    fn tick(&mut self, debounce_secs: u64) -> Result<()> {
        // Check git ref changes (2s debounce to batch rapid ref updates)
        if self.git_dirty {
            if let Some(last) = self.last_git_event {
                if last.elapsed() > Duration::from_secs(2) {
                    self.git_dirty = false;
                    self.refresh_gitbutler_status()?;
                }
            }
        }

        // Check agent idle debounce
        if !self.agent_idle {
            if let Some(last) = self.last_transcript_activity {
                if last.elapsed() > Duration::from_secs(debounce_secs) {
                    self.on_agent_stop()?;
                }
            }
        }

        Ok(())
    }

    fn on_agent_stop(&mut self) -> Result<()> {
        self.agent_idle = true;
        let idle_secs = self
            .last_transcript_activity
            .map_or(0, |t| t.elapsed().as_secs());

        eprintln!("\n{BOLD}{YELLOW}[idle]{RESET} Agent stopped ({idle_secs}s) -- committing...");

        // Run but commit --ai --json
        let output = Command::new("but")
            .args(["commit", "--ai", "--json"])
            .current_dir(&self.workdir)
            .output()
            .context("failed to run 'but commit --ai --json'")?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        if !output.status.success() {
            let msg = if !stderr.is_empty() {
                stderr.trim().to_string()
            } else {
                stdout.trim().to_string()
            };
            eprintln!("  {}[commit]{} Failed: {}", RED, RESET, truncate(&msg, 120));
            return Ok(());
        }

        // Try to parse JSON output
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&stdout) {
            // Use embedded status if available
            if !json["status"].is_null() {
                self.process_status_update(&json["status"])?;
            } else {
                self.refresh_gitbutler_status()?;
            }
        } else {
            // Non-JSON output, just refresh status
            eprintln!(
                "  {}[commit]{} {}",
                GREEN,
                RESET,
                truncate(stdout.trim(), 120)
            );
            self.refresh_gitbutler_status()?;
        }

        // Attach transcript to the branch
        self.attach_transcript()?;

        Ok(())
    }

    fn refresh_gitbutler_status(&mut self) -> Result<()> {
        let output = Command::new("but")
            .args(["status", "--json"])
            .current_dir(&self.workdir)
            .output()
            .context("failed to run 'but status --json'")?;

        if !output.status.success() {
            return Ok(());
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let status: serde_json::Value = match serde_json::from_str(&stdout) {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };

        self.process_status_update(&status)
    }

    fn process_status_update(&mut self, status: &serde_json::Value) -> Result<()> {
        let Some(stacks) = status["stacks"].as_array() else {
            return Ok(());
        };

        let session = Session::discover()?;
        let db = session.store();
        let email = session.email();

        for stack in stacks {
            let Some(branches) = stack["branches"].as_array() else {
                continue;
            };

            for branch in branches {
                let branch_name = match branch["name"].as_str() {
                    Some(n) => n.to_string(),
                    None => continue,
                };

                let Some(commits) = branch["commits"].as_array() else {
                    continue;
                };

                for commit in commits {
                    let commit_id = match commit["commitId"].as_str() {
                        Some(id) => id.to_string(),
                        None => continue,
                    };

                    if self.known_commits.contains(&commit_id) {
                        continue;
                    }

                    self.known_commits.insert(commit_id.clone());
                    self.branch_for_commit
                        .insert(commit_id.clone(), branch_name.clone());
                    self.last_committed_branch = Some(branch_name.clone());

                    // Get change-id via but show
                    let cli_id = commit["cliId"].as_str().unwrap_or("");
                    let show_id = if !cli_id.is_empty() {
                        cli_id
                    } else {
                        &commit_id
                    };

                    let change_id = get_change_id(&self.workdir, show_id);

                    let short_sha = &commit_id[..8.min(commit_id.len())];
                    let msg = commit["message"].as_str().unwrap_or("");
                    let msg_preview = truncate(msg, 60);

                    eprintln!(
                        "  {BOLD}{GREEN}[commit]{RESET} {BOLD}{short_sha}{RESET} {branch_name} \"{msg_preview}\""
                    );

                    if let Some(ref cid) = change_id {
                        let ts =
                            OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000;
                        let first_seen = *self
                            .branch_first_seen
                            .entry(branch_name.clone())
                            .or_insert(ts);
                        let branch_id = format!("{branch_name}@{first_seen}");
                        let value = serde_json::to_string(&branch_id)?;
                        let cid_target =
                            Target::from_parts(TargetType::ChangeId, Some(cid.clone()));
                        db.set(
                            &cid_target,
                            "branch:id",
                            &value,
                            &ValueType::String,
                            email,
                            ts,
                        )?;

                        let short_cid = &cid[..16.min(cid.len())];
                        eprintln!(
                            "  {CYAN}[meta]{RESET} change-id:{short_cid}... branch:id = {branch_id}"
                        );

                        // Attach new user prompts to the change-id
                        let prompts = self.extract_new_user_prompts();
                        if !prompts.is_empty() {
                            let prompt_count = prompts.len();
                            for prompt in prompts {
                                db.list_push_with_repo(
                                    Some(session.repo()),
                                    &cid_target,
                                    "agent:prompts",
                                    &prompt,
                                    email,
                                    ts,
                                )?;
                            }
                            eprintln!(
                                "  {CYAN}[meta]{RESET} change-id:{short_cid}... agent:prompts += {prompt_count} prompt(s)"
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Extract user prompt texts from session lines that haven't been attached yet.
    fn extract_new_user_prompts(&mut self) -> Vec<String> {
        let Some(session_id) = &self.active_session_id else {
            return Vec::new();
        };
        let session_id = session_id.clone();

        let Some(lines) = self.session_lines.get(&session_id) else {
            return Vec::new();
        };

        let start = self
            .prompts_attached_up_to
            .get(&session_id)
            .copied()
            .unwrap_or(0);

        let mut prompts = Vec::new();
        for line in lines.iter().skip(start) {
            let parsed: serde_json::Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(_) => continue,
            };

            if parsed["type"].as_str() != Some("user") {
                continue;
            }
            if parsed["isMeta"].as_bool().unwrap_or(false) {
                continue;
            }

            for text in extract_content_texts(&parsed["message"]["content"]) {
                prompts.push(text);
            }
        }

        // Mark all current lines as processed
        self.prompts_attached_up_to.insert(session_id, lines.len());

        prompts
    }

    fn attach_transcript(&mut self) -> Result<()> {
        let session_id = match &self.active_session_id {
            Some(id) => id.clone(),
            None => return Ok(()),
        };

        let lines = match self.session_lines.get(&session_id) {
            Some(l) if !l.is_empty() => l.clone(),
            _ => return Ok(()),
        };

        let branch_name = match &self.last_committed_branch {
            Some(b) => b.clone(),
            None => {
                eprintln!("  {YELLOW}[warn]{RESET} No branch found to attach transcript to");
                return Ok(());
            }
        };

        let transcript_content = lines.join("\n");
        let ts = OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000;

        let first_seen = *self
            .branch_first_seen
            .entry(branch_name.clone())
            .or_insert(ts);
        let branch_id = format!("{branch_name}@{first_seen}");

        let session = Session::discover()?;
        let db = session.store();
        let email = session.email();

        let branch_target = Target::from_parts(TargetType::Branch, Some(branch_id.clone()));
        db.list_push_with_repo(
            Some(session.repo()),
            &branch_target,
            "agent:transcripts",
            &transcript_content,
            email,
            ts,
        )?;

        eprintln!(
            "  {}{}[meta]{} Stored {} transcript lines -> branch:{} agent:transcripts",
            BOLD,
            GREEN,
            RESET,
            lines.len(),
            branch_id
        );

        // Clear stored lines -- they've been persisted
        self.session_lines.remove(&session_id);

        Ok(())
    }
}

fn get_change_id(workdir: &Path, show_id: &str) -> Option<String> {
    let output = Command::new("but")
        .args(["show", show_id, "--json"])
        .current_dir(workdir)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).ok()?;
    json["changeId"]
        .as_str()
        .map(std::string::ToString::to_string)
}

/// Extract text strings from a message content value.
/// Handles both the array-of-blocks format (`[{"type":"text","text":"..."}]`)
/// and the plain-string format (`"the prompt text"`) used by Claude Code transcripts.
fn extract_content_texts(content: &serde_json::Value) -> Vec<String> {
    let mut texts = Vec::new();
    if let Some(arr) = content.as_array() {
        for block in arr {
            if block["type"].as_str() == Some("text") {
                if let Some(text) = block["text"].as_str() {
                    let text = text.trim();
                    if !text.is_empty() {
                        texts.push(text.to_string());
                    }
                }
            }
        }
    } else if let Some(s) = content.as_str() {
        let s = s.trim();
        if !s.is_empty() {
            texts.push(s.to_string());
        }
    }
    texts
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() > max {
        format!("{}...", &s[..max])
    } else {
        s.to_string()
    }
}
