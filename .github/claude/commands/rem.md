Store a memory about the current project.

Usage: /rem "<content>" [topic]

Steps:
1. Parse `$ARGUMENTS`: the content is the quoted string (everything inside the first pair of double quotes). The topic is the optional word after the closing quote. If no topic is provided, infer a short CamelCase topic word from the content.
2. Determine the current project name from the working directory (use the repo root folder name, or "global" if not in a git repo).
3. Build the path: `~/.claude/memory/<project>/memory-<topic>.md`
4. If the file already exists, append the new content under a `---` separator with today's date as a heading. If it does not exist, create it with a `# <topic>` heading followed by the content.
5. Confirm to the user which file was written and what was stored.
