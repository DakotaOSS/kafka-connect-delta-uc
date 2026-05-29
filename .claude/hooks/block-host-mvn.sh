#!/usr/bin/env bash
# PreToolUse(Bash|PowerShell): block host Maven. hadoop-azure needs winutils/HADOOP_HOME on Windows, so
# the suite runs only inside the maven Docker image (see CLAUDE.md / the /test skill). mvn is allowed
# when it goes through `docker run`.
#
# Opt-in and Windows-specific: wired via .claude/settings.local.json (gitignored), not shipped to
# Linux/Mac contributors who can run mvn natively. Exit 2 blocks the tool call and shows the message.
input=$(cat)

# Anything routed through docker is fine.
if printf '%s' "$input" | grep -qi 'docker'; then
  exit 0
fi

# Block mvn / mvnw only when invoked as a command (command start, or after && ; |) so commit messages
# or greps that merely mention "mvn" don't trip it.
if printf '%s' "$input" \
  | grep -qE '("command"[[:space:]]*:[[:space:]]*"|&&[[:space:]]*|;[[:space:]]*|\|[[:space:]]*)mvnw?([[:space:]"]|$)'; then
  echo "host 'mvn' is unsupported here: hadoop-azure needs winutils/HADOOP_HOME on Windows. Run the suite via the Docker command in CLAUDE.md, or the /test skill." >&2
  exit 2
fi
exit 0
