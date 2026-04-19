#!/usr/bin/env ruby

# Scans Claude Code transcripts for a project, finds commits that were
# created during those sessions, and attaches real session metadata via git meta.
#
# Strategy for finding commits:
#   1. Look for `git commit` / `but commit` tool calls in the transcript and
#      try to match by commit message or SHA in the tool output.
#   2. Look for commits with a "Co-Authored-By: Claude" trailer whose
#      author date falls within the session's time window.
#   3. Fall back to the user's closest commit after the session ended.
#
# When the commit message contains a Change-Id trailer, metadata is attached
# to `change-id:<id>` instead of `commit:<sha>`.
#
# Usage: ruby scripts/backlog.rb --path=REPO_PATH
#        ruby scripts/backlog.rb                    # defaults to .

require 'json'
require 'time'
require 'optparse'
require 'set'

repo_path = "."
dry_run = false

parser = OptionParser.new do |opts|
  opts.banner = "Usage: ruby scripts/backlog.rb [--path=REPO_PATH] [--dry-run]"
  opts.on("--path=PATH", "Path to the git repository (default: .)") { |p| repo_path = p }
  opts.on("--dry-run", "Show what would be done without writing metadata") { dry_run = true }
  opts.on("-h", "--help", "Show this help") { puts opts; exit }
end
parser.parse!

repo_path = File.expand_path(repo_path)

unless Dir.exist?(File.join(repo_path, ".git"))
  abort "Error: #{repo_path} is not a git repository"
end

Dir.chdir(repo_path)

# ---------------------------------------------------------------------------
# Resolve the Claude transcripts directory for this repo
# ---------------------------------------------------------------------------

claude_base = File.expand_path("~/.claude/projects")
# Claude Code uses the absolute path with / replaced by -
project_key = repo_path.gsub("/", "-")
transcripts_dir = File.join(claude_base, project_key)

unless Dir.exist?(transcripts_dir)
  abort "Error: no Claude transcripts found at #{transcripts_dir}"
end

jsonl_files = Dir.glob(File.join(transcripts_dir, "*.jsonl")).sort
if jsonl_files.empty?
  abort "Error: no .jsonl transcript files in #{transcripts_dir}"
end

puts "Found #{jsonl_files.length} transcript(s) in #{transcripts_dir}"
puts "Repository: #{repo_path}"
puts "Dry run: #{dry_run}" if dry_run
puts

# ---------------------------------------------------------------------------
# Build a map of recent commits: sha -> { date, message, author, change_id }
# ---------------------------------------------------------------------------

puts "Loading commit history..."

commit_map = {}
# Get last 5000 commits with full message body
raw = `git log --format="COMMIT_START%n%H%n%aI%n%an%n%ae%n%B%nCOMMIT_END" -5000 2>/dev/null`
raw.split("COMMIT_START\n").each do |block|
  next if block.strip.empty?
  parts = block.split("\n")
  next if parts.length < 5
  sha = parts[0]
  date = parts[1]
  author_name = parts[2]
  author_email = parts[3]
  body = parts[4..].join("\n").sub(/\nCOMMIT_END\s*\z/, "")

  change_id = nil
  if body =~ /^Change-Id:\s*(\S+)/m
    change_id = $1
  end

  has_claude_trailer = body =~ /Co-Authored-By:.*Claude/i

  commit_map[sha] = {
    date: Time.parse(date),
    author_name: author_name,
    author_email: author_email,
    message: body.strip,
    change_id: change_id,
    has_claude_trailer: has_claude_trailer,
  }
end

puts "Loaded #{commit_map.length} commits"

# Get the repo owner's email for fallback matching
user_email = `git config user.email`.strip

# ---------------------------------------------------------------------------
# Parse a single transcript file
# ---------------------------------------------------------------------------

def parse_transcript(path)
  entries = []
  File.foreach(path) do |line|
    entries << JSON.parse(line.strip)
  rescue JSON::ParserError
    next
  end

  session_id = nil
  timestamps = []
  models = Set.new
  tools_used = Set.new
  total_input_tokens = 0
  total_output_tokens = 0
  messages = []
  commit_commands = []

  entries.each do |obj|
    ts = obj["timestamp"]
    timestamps << Time.parse(ts) if ts

    session_id ||= obj["sessionId"]

    case obj["type"]
    when "assistant"
      msg = obj.dig("message") || {}
      model = msg["model"]
      models.add(model) if model && model != "<synthetic>"

      usage = msg["usage"] || {}
      total_input_tokens += usage["input_tokens"].to_i
      total_output_tokens += usage["output_tokens"].to_i

      (msg["content"] || []).each do |c|
        next unless c.is_a?(Hash)
        case c["type"]
        when "text"
          text = c["text"].to_s.strip
          messages << { role: "assistant", content: text } unless text.empty?
        when "tool_use"
          tool_name = c["name"]
          tools_used.add(tool_name) if tool_name
          input = c["input"] || {}
          cmd = input["command"].to_s
          # Track git/but commit commands
          if cmd =~ /\b(git|but)\s+commit\b/
            commit_commands << { command: cmd, tool_use_id: c["id"] }
          end
        end
      end

    when "user"
      msg = obj.dig("message") || {}
      content = msg["content"]
      if content.is_a?(String) && !content.strip.empty?
        messages << { role: "user", content: content.strip }
      elsif content.is_a?(Array)
        content.each do |c|
          next unless c.is_a?(Hash)
          if c["type"] == "text"
            text = c["text"].to_s.strip
            messages << { role: "user", content: text } unless text.empty?
          elsif c["type"] == "tool_result"
            # Check tool results for commit SHA output
            result_text = c["content"].to_s
            commit_commands.each do |cc|
              if cc[:tool_use_id] == c["tool_use_id"]
                cc[:result] = result_text
              end
            end
          end
        end
      end
    end
  end

  return nil if timestamps.empty?

  {
    session_id: session_id,
    path: path,
    start_time: timestamps.min,
    end_time: timestamps.max,
    models: models.to_a,
    tools_used: tools_used.to_a.sort,
    total_input_tokens: total_input_tokens,
    total_output_tokens: total_output_tokens,
    messages: messages,
    commit_commands: commit_commands,
    duration_secs: (timestamps.max - timestamps.min).to_i,
  }
end

# ---------------------------------------------------------------------------
# Find commits associated with a session
# ---------------------------------------------------------------------------

def find_commits_for_session(session, commit_map, user_email)
  matches = []
  seen = Set.new

  start_time = session[:start_time]
  end_time = session[:end_time]
  # Give a small buffer around the session window
  window_start = start_time - 60
  window_end = end_time + 300 # 5 min after session ends

  # Strategy 1: Look for commit SHAs mentioned in tool results of commit commands
  session[:commit_commands].each do |cc|
    result = cc[:result].to_s
    # Look for full SHA in output
    commit_map.each_key do |sha|
      if result.include?(sha) || result.include?(sha[0..6])
        unless seen.include?(sha)
          matches << { sha: sha, method: "tool_output" }
          seen.add(sha)
        end
      end
    end
  end

  # Strategy 2: Commits with Claude trailer within the time window
  commit_map.each do |sha, info|
    next if seen.include?(sha)
    next unless info[:has_claude_trailer]
    if info[:date] >= window_start && info[:date] <= window_end
      matches << { sha: sha, method: "claude_trailer" }
      seen.add(sha)
    end
  end

  # Strategy 3: Commits by the user within the session time window
  commit_map.each do |sha, info|
    next if seen.include?(sha)
    next unless info[:author_email] == user_email
    if info[:date] >= window_start && info[:date] <= window_end
      matches << { sha: sha, method: "time_window" }
      seen.add(sha)
    end
  end

  # Strategy 4: If nothing found, find the user's closest commit after session end
  if matches.empty?
    best_sha = nil
    best_delta = Float::INFINITY
    commit_map.each do |sha, info|
      next unless info[:author_email] == user_email
      delta = (info[:date] - end_time).to_f
      # Must be after the session ended, within 1 hour
      if delta >= 0 && delta < 3600 && delta < best_delta
        best_sha = sha
        best_delta = delta
      end
    end
    if best_sha
      matches << { sha: best_sha, method: "closest_after" }
    end
  end

  matches
end

# ---------------------------------------------------------------------------
# Write metadata for a commit
# ---------------------------------------------------------------------------

def meta_target(sha, commit_map)
  info = commit_map[sha]
  if info && info[:change_id]
    "change-id:#{info[:change_id]}"
  else
    "commit:#{sha}"
  end
end

def write_metadata(target, session, dry_run)
  model = session[:models].first || "unknown"
  cmds = [
    ["git", "meta", "set", target, "agent:session", session[:session_id]],
    ["git", "meta", "set", target, "agent:model", model],
    ["git", "meta", "set", target, "agent:usage:input_tokens", session[:total_input_tokens].to_s],
    ["git", "meta", "set", target, "agent:usage:output_tokens", session[:total_output_tokens].to_s],
    ["git", "meta", "set", target, "agent:duration_secs", session[:duration_secs].to_s],
  ]

  # Tools used as a list
  session[:tools_used].each do |tool|
    cmds << ["git", "meta", "list:push", target, "agent:tools_used", tool]
  end

  # Transcript: store each message as a list entry
  transcript_json = session[:messages].map { |m| JSON.generate(m) }
  unless transcript_json.empty?
    cmds << ["git", "meta", "set", "-t", "list", target, "agent:transcript", JSON.generate(transcript_json)]
  end

  cmds.each do |cmd|
    if dry_run
      puts "  $ #{cmd.join(' ')}"
    else
      system(*cmd)
    end
  end
end

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

tagged_count = 0
skipped_count = 0
no_match_count = 0

jsonl_files.each_with_index do |path, idx|
  session = parse_transcript(path)
  next unless session
  next unless session[:session_id]

  # Skip very short sessions (no real conversation)
  if session[:messages].length < 2
    skipped_count += 1
    next
  end

  basename = File.basename(path)
  matches = find_commits_for_session(session, commit_map, user_email)

  if matches.empty?
    no_match_count += 1
    puts "[#{idx + 1}/#{jsonl_files.length}] #{basename} — no matching commits found (#{session[:start_time].strftime('%Y-%m-%d %H:%M')})"
    next
  end

  matches.each do |match|
    sha = match[:sha]
    target = meta_target(sha, commit_map)

    # Skip if already tagged
    existing = `git meta get #{target} agent:session 2>/dev/null`.strip
    unless existing.empty?
      skipped_count += 1
      puts "[#{idx + 1}/#{jsonl_files.length}] #{basename} — #{target} already tagged, skipping"
      next
    end

    info = commit_map[sha]
    msg_preview = (info[:message] || "").lines.first.to_s.strip[0..60]
    model = session[:models].first || "?"
    puts "[#{idx + 1}/#{jsonl_files.length}] #{basename} -> #{target} (#{match[:method]}) #{model}"
    puts "  commit: #{msg_preview}"

    write_metadata(target, session, dry_run)
    tagged_count += 1
  end
end

puts
puts "Done!"
puts "  Tagged:     #{tagged_count} commits"
puts "  Skipped:    #{skipped_count} (already tagged or too short)"
puts "  No match:   #{no_match_count} sessions with no commit found"
puts
puts "Inspect with: git meta get commit:<sha>" unless dry_run
