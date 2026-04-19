#!/usr/bin/env ruby

# Pretty-prints git log with git meta metadata for each commit.
#
# For each commit, shows: SHA, author, first 4 lines of the message,
# then all metadata keys with a preview of the value.
#
# Usage: ruby scripts/log.rb [--path=REPO_PATH] [-n COUNT]

require 'json'
require 'optparse'

repo_path = "."
count = 20
metadata_only = false

parser = OptionParser.new do |opts|
  opts.banner = "Usage: ruby scripts/log.rb [--path=REPO_PATH] [-n COUNT] [--mo]"
  opts.on("--path=PATH", "Path to the git repository (default: .)") { |p| repo_path = p }
  opts.on("-n COUNT", Integer, "Number of commits to show (default: 20)") { |n| count = n }
  opts.on("--mo", "Metadata only: show only commits that have metadata") { metadata_only = true }
end
parser.parse!(ARGV)

# ANSI color helpers
module C
  RESET   = "\e[0m"
  BOLD    = "\e[1m"
  DIM     = "\e[2m"
  YELLOW  = "\e[33m"
  GREEN   = "\e[32m"
  CYAN    = "\e[36m"
  MAGENTA = "\e[35m"
  BLUE    = "\e[34m"
  WHITE   = "\e[37m"
  RED     = "\e[31m"

  def self.yellow(s)  "#{YELLOW}#{s}#{RESET}" end
  def self.green(s)   "#{GREEN}#{s}#{RESET}" end
  def self.cyan(s)    "#{CYAN}#{s}#{RESET}" end
  def self.magenta(s) "#{MAGENTA}#{s}#{RESET}" end
  def self.blue(s)    "#{BLUE}#{s}#{RESET}" end
  def self.dim(s)     "#{DIM}#{s}#{RESET}" end
  def self.bold(s)    "#{BOLD}#{s}#{RESET}" end
  def self.red(s)     "#{RED}#{s}#{RESET}" end
end

def format_value_preview(value)
  # Try to parse as JSON first
  parsed = begin
    JSON.parse(value)
  rescue
    nil
  end

  if parsed.is_a?(Array)
    "#{C.magenta("[list: #{parsed.length} items]")}"
  elsif parsed.is_a?(Hash)
    "#{C.magenta("{object: #{parsed.keys.length} keys}")}"
  else
    # Treat as string — show first 50 chars or until first newline
    str = value.to_s
    first_line = str.split("\n", 2).first || ""
    preview = first_line.length > 50 ? first_line[0...50] + "..." : first_line
    preview += " ..." if str.include?("\n") && first_line.length <= 50
    C.dim(preview)
  end
end

# Get commit list
log_format = "%H%x00%an%x00%ae%x00%B%x00"
raw = `git -C #{repo_path} log --format="#{log_format}" -n #{count} 2>&1`

if !$?.success?
  $stderr.puts "Error running git log: #{raw}"
  exit 1
end

# Parse commits — split on the record separator
commits = raw.split("\0\n").reject(&:empty?).each_slice(1).map do |chunk|
  line = chunk.join
  parts = line.split("\0", 4)
  next if parts.length < 4
  {
    sha: parts[0].strip,
    author: parts[1].strip,
    email: parts[2].strip,
    message: parts[3].strip
  }
end.compact

if commits.empty?
  puts "No commits found."
  exit 0
end

printed = 0
commits.each do |commit|
  sha = commit[:sha]
  short_sha = sha[0...10]

  # Fetch metadata early so we can skip if --mo is set
  meta = nil
  meta_raw = `git meta get --json commit:#{sha} 2>/dev/null`
  if $?.success? && !meta_raw.strip.empty? && meta_raw.strip != "{}"
    meta = begin
      JSON.parse(meta_raw)
    rescue
      nil
    end
    meta = nil if meta && meta.empty?
  end

  next if metadata_only && meta.nil?

  puts "" if printed > 0
  printed += 1

  # Header
  puts "#{C.yellow("commit #{short_sha}")} #{C.dim("—")} #{C.green(commit[:author])} #{C.dim("<#{commit[:email]}>")}"

  # Message (first 4 lines)
  lines = commit[:message].split("\n").reject(&:empty?)
  msg_lines = lines[0...4]
  msg_lines.each do |line|
    puts "  #{line}"
  end
  if lines.length > 4
    puts "  #{C.dim("... (#{lines.length - 4} more lines)")}"
  end

  # Metadata display
  if meta
    puts "  #{C.cyan("╶── metadata ──")}"
    flatten_json("", meta).each do |key, value|
      preview = format_value_preview(value.is_a?(String) ? value : JSON.generate(value))
      puts "  #{C.blue("│")} #{C.bold(key)}  #{preview}"
    end
    puts "  #{C.blue("╵")}"
  end
end

# Recursively flatten nested JSON into dot-separated keys
BEGIN {
  def flatten_json(prefix, obj)
    results = []
    case obj
    when Hash
      obj.each do |k, v|
        full_key = prefix.empty? ? k : "#{prefix}:#{k}"
        if v.is_a?(Hash)
          results.concat(flatten_json(full_key, v))
        else
          results << [full_key, v]
        end
      end
    else
      results << [prefix, obj]
    end
    results
  end
}
