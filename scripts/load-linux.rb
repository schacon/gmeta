#!/usr/bin/env ruby

# Attaches real Claude/Codex transcripts to commits in a git repository
# that don't already have an agent:session key.
#
# Transcript sources (all shuffled together):
#   - transcripts/ directory in this repo (~1700 real-world transcripts)
#   - ~/.claude/projects/*/*.jsonl (local Claude Code sessions)
#   - ~/.codex/sessions/**/*.jsonl  (local Codex sessions)
#
# Usage: ruby scripts/load-linux.rb [--path=REPO_PATH] [COUNT]
#   REPO_PATH defaults to ../linux
#   COUNT     defaults to 100

require 'json'
require 'optparse'
require 'set'

repo_path = File.expand_path("../../linux", __dir__)
count = 100

parser = OptionParser.new do |opts|
  opts.banner = "Usage: ruby scripts/load-linux.rb [--path=REPO_PATH] [COUNT]"
  opts.on("--path=PATH", "Path to the git repository (default: ../linux)") { |p| repo_path = File.expand_path(p) }
  opts.on("-h", "--help", "Show this help") { puts opts; exit }
end
parser.parse!

count = ARGV[0].to_i if ARGV[0]
count = 100 if count <= 0

unless Dir.exist?(File.join(repo_path, ".git"))
  abort "Error: #{repo_path} is not a git repository"
end

# ---------------------------------------------------------------------------
# Collect all real transcripts from ~/.claude and ~/.codex
# ---------------------------------------------------------------------------

def parse_claude_transcript(path)
  entries = []
  File.foreach(path) do |line|
    entries << JSON.parse(line.strip)
  rescue JSON::ParserError
    next
  end
  return nil if entries.empty?

  session_id = nil
  timestamps = []
  models = Set.new
  tools_used = Set.new
  total_input_tokens = 0
  total_output_tokens = 0
  messages = []

  entries.each do |obj|
    ts = obj["timestamp"]
    timestamps << ts if ts
    session_id ||= obj["sessionId"]

    case obj["type"]
    when "assistant"
      msg = obj["message"] || {}
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
          tools_used.add(c["name"]) if c["name"]
          messages << { role: "assistant", tool_use: { tool: c["name"], input: summarize_input(c["input"]) } }
        end
      end

    when "user"
      msg = obj["message"] || {}
      content = msg["content"]
      if content.is_a?(String) && !content.strip.empty?
        messages << { role: "user", content: content.strip }
      elsif content.is_a?(Array)
        content.each do |c|
          next unless c.is_a?(Hash)
          if c["type"] == "text"
            text = c["text"].to_s.strip
            messages << { role: "user", content: text } unless text.empty?
          end
        end
      end
    end
  end

  return nil if messages.length < 2

  {
    source: "claude",
    session_id: session_id || File.basename(path, ".jsonl"),
    models: models.to_a,
    tools_used: tools_used.to_a.sort,
    total_input_tokens: total_input_tokens,
    total_output_tokens: total_output_tokens,
    messages: messages,
    duration_secs: compute_duration(timestamps),
    message_count: messages.length,
  }
end

def parse_codex_transcript(path)
  entries = []
  File.foreach(path) do |line|
    entries << JSON.parse(line.strip)
  rescue JSON::ParserError
    next
  end
  return nil if entries.empty?

  session_id = nil
  messages = []
  tools_used = Set.new

  entries.each do |obj|
    # First line is the session header with an "id" field
    if obj["id"] && !obj["type"]
      session_id = obj["id"]
      next
    end

    case obj["type"]
    when "message"
      role = obj["role"]
      (obj["content"] || []).each do |c|
        next unless c.is_a?(Hash)
        case c["type"]
        when "input_text", "output_text"
          text = c["text"].to_s.strip
          messages << { role: role, content: text } unless text.empty?
        end
      end
    when "function_call"
      name = obj["name"]
      tools_used.add(name) if name
      messages << { role: "assistant", tool_use: { tool: name, input: obj["arguments"].to_s[0..200] } }
    when "function_call_output"
      # skip raw output, too large
    end
  end

  return nil if messages.length < 2

  {
    source: "codex",
    session_id: session_id || File.basename(path, ".jsonl"),
    models: ["codex"],
    tools_used: tools_used.to_a.sort,
    total_input_tokens: 0,
    total_output_tokens: 0,
    messages: messages,
    duration_secs: 0,
    message_count: messages.length,
  }
end

def summarize_input(input)
  return "" unless input.is_a?(Hash)
  # Keep it short — just the command or file path
  input["command"] || input["file_path"] || input["pattern"] || input["prompt"]&.slice(0, 120) || ""
end

def compute_duration(timestamps)
  return 0 if timestamps.length < 2
  times = timestamps.map { |t| Time.parse(t) rescue nil }.compact
  return 0 if times.length < 2
  (times.max - times.min).to_i
end

puts "Collecting transcripts..."

all_transcripts = []
source_counts = Hash.new(0)

# Bundled transcripts in transcripts/ directory (same JSONL format as Claude)
bundled_dir = File.join(File.dirname(__dir__), "transcripts")
if Dir.exist?(bundled_dir)
  files = Dir.glob(File.join(bundled_dir, "*.transcript.json"))
  puts "  Scanning #{files.length} bundled transcripts..."
  files.each do |path|
    t = parse_claude_transcript(path)
    if t
      t[:source] = "bundled"
      all_transcripts << t
      source_counts["bundled"] += 1
    end
  end
end

# Local Claude transcripts
claude_projects = File.expand_path("~/.claude/projects")
if Dir.exist?(claude_projects)
  files = Dir.glob(File.join(claude_projects, "*", "*.jsonl"))
  puts "  Scanning #{files.length} local Claude transcripts..."
  files.each do |path|
    t = parse_claude_transcript(path)
    if t
      all_transcripts << t
      source_counts["claude"] += 1
    end
  end
end

# Local Codex transcripts
codex_sessions = File.expand_path("~/.codex/sessions")
if Dir.exist?(codex_sessions)
  files = Dir.glob(File.join(codex_sessions, "**", "*.jsonl"))
  puts "  Scanning #{files.length} local Codex transcripts..."
  files.each do |path|
    t = parse_codex_transcript(path)
    if t
      all_transcripts << t
      source_counts["codex"] += 1
    end
  end
end

if all_transcripts.empty?
  abort "Error: no transcripts found"
end

summary = source_counts.map { |k, v| "#{v} #{k}" }.join(", ")
puts "Found #{all_transcripts.length} transcripts (#{summary})"

# Shuffle so we get a good mix
all_transcripts.shuffle!

# ---------------------------------------------------------------------------
# Find commits that don't already have agent:session
# ---------------------------------------------------------------------------

Dir.chdir(repo_path)

puts "Scanning commits in #{repo_path} for ones without agent:session..."

batch = [count * 2, 500].min
candidates = []
offset = 0

while candidates.length < count
  shas = `git log --format=%H --skip=#{offset} -#{batch}`.strip.split("\n")
  break if shas.empty?

  shas.each do |sha|
    existing = `git meta get commit:#{sha} agent:session 2>/dev/null`.strip
    if existing.empty?
      candidates << sha
      break if candidates.length >= count
    end
  end

  offset += batch
end

if candidates.empty?
  abort "Error: no un-tagged commits found in #{repo_path}"
end

if candidates.length < count
  puts "Warning: only found #{candidates.length} commits without agent:session (requested #{count})"
end

puts "Found #{candidates.length} commits to tag\n\n"

# ---------------------------------------------------------------------------
# Attach real transcripts to commits
# ---------------------------------------------------------------------------

used_transcripts = {}

candidates.each_with_index do |sha, idx|
  # Pick an unused transcript if possible, otherwise reset and reuse
  available = all_transcripts.reject { |t| used_transcripts[t[:session_id]] }
  if available.empty?
    used_transcripts.clear
    available = all_transcripts
  end
  transcript = available.sample
  used_transcripts[transcript[:session_id]] = true

  model = transcript[:models].first || "unknown"

  # Core metadata
  system("git", "meta", "set", "commit:#{sha}", "agent:session", transcript[:session_id])
  system("git", "meta", "set", "commit:#{sha}", "agent:model", model)
  system("git", "meta", "set", "commit:#{sha}", "agent:source", transcript[:source])

  # Transcript as a list of JSON message strings
  transcript_json = transcript[:messages].map { |msg| JSON.generate(msg) }
  system("git", "meta", "set", "-t", "list", "commit:#{sha}", "agent:transcript", JSON.generate(transcript_json))

  # Token usage
  if transcript[:total_input_tokens] > 0
    system("git", "meta", "set", "commit:#{sha}", "agent:usage:input_tokens", transcript[:total_input_tokens].to_s)
    system("git", "meta", "set", "commit:#{sha}", "agent:usage:output_tokens", transcript[:total_output_tokens].to_s)
  end

  # Duration
  if transcript[:duration_secs] > 0
    system("git", "meta", "set", "commit:#{sha}", "agent:duration_secs", transcript[:duration_secs].to_s)
  end

  # Tools used
  transcript[:tools_used].each do |tool|
    system("git", "meta", "list:push", "commit:#{sha}", "agent:tools_used", tool)
  end

  puts "[#{idx + 1}/#{candidates.length}] #{sha[0..9]} <- #{transcript[:source]}:#{transcript[:session_id][0..7]} (#{model}, #{transcript[:message_count]} msgs)"
end

puts "\nDone! Attached real transcripts to #{candidates.length} commits."
puts "Inspect with: cd #{repo_path} && git meta get commit:<sha>"
