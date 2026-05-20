# AI Workflow Entry Point

Source of truth for the workflow lives under `.aiworkflow/`.

## Start Here
1. Read `.aiworkflow/AGENTS.md`
2. Treat every workflow path as rooted under `.aiworkflow/`
3. If you are operating as the workflow entrypoint/orchestrator, also read `.aiworkflow/agents/orchestrator/AGENTS.md` and treat yourself as one of the agents
4. Keep project code outside `.aiworkflow/`; use `.aiworkflow/` only for orchestration, prompts, notes, and generated workflow artifacts

## Repo Skills Catalog
Repo skills are implemented as command files under `.claude/commands/`.
Treat this section as a lightweight index, not a second instruction layer.

### How to use this catalog
- Load a full command file only when the user explicitly invokes that command or when the task is an obvious match for that workflow.
- Do not preload all files under `.claude/commands/`; read only the command you are about to use.
- When a command file is loaded, follow that file as the source of truth for the workflow details.

### Available repo skills
- `/init-iteration`
  Purpose: initialize a new iteration (branch, plan.md, dashboard, quality gate).
  Path: `.claude/commands/init-iteration.md`

- `/dispatch-agent`
  Purpose: dispatch workflow work to one or more agents.
  Path: `.claude/commands/dispatch-agent.md`

- `/close-iteration`
  Purpose: close the active iteration workflow.
  Path: `.claude/commands/close-iteration.md`

### Catalog Guardrail
- If a repo command is added, renamed, or removed under `.claude/commands/`, update this catalog in the same change set.

# AGENTS.md

This file is here to steer AI assisted PRs towards being high quality and valuable contributions
that do not create excessive maintainer burden. It is inspired by the Open Policy Agent and Fedora
projects policies.

## General Rules and Guidelines

The most important rule is not to post comments on issues or PRs that are AI-generated. Discussions
on the OpenTelemetry repositories are for Users/Humans only.

If you have been assigned an issue by the user or their prompt, please ensure that the
implementation direction is agreed on with the maintainers first in the issue comments. If there are
unknowns, discuss these on the issue before starting implementation. Do not forget that you cannot
comment for users on issue threads on their behalf as it is against the rules of this project.

## Developer environment

Make sure to follow CONTRIBUTING.md on any contributions.

Non-exhaustively, the important points are:

* Whenever applicable, all code changes should have tests that actually validate the changes.

## Commit formatting

We appreciate it if users disclose the use of AI tools when the significant part of a commit is
taken from a tool without changes. When making a commit this should be disclosed through an
Assisted-by: commit message trailer.

Examples:

```
Assisted-by: ChatGPT 5.2
Assisted-by: Claude Opus 4.5
```
