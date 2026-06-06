Store a memory about the current project.

Usage: /rem "<content>" [Topic]

Steps:
1. Parse `$ARGUMENTS`: the content is the quoted string (everything inside the first pair of double quotes). The topic is the optional CamelCase word after the closing quote. If no topic is provided, infer a short CamelCase topic word from the content.
2. Determine the project name using the first available source:
   a. GitHub project name (from `gh repo view --json name` or MCP context)
   b. Git repo root folder name (`git rev-parse --show-toplevel`)
   c. Inferred from the current AI session context (e.g. what the conversation is about)
   d. "global" as last resort
3. Build the path: `~/.claude/memory/<project>/memory-<Topic>.md`
4. If the file already exists, append the new content under a `---` separator with today's date as a heading. If it does not exist, create it with a `# <Topic>` heading followed by the content.
5. Confirm to the user which file was written and what was stored.
