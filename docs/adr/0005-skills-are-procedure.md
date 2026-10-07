# 0005. Skills are procedure, `CLAUDE.md` is convention

- Status: Accepted
- Date: 2026-10-07

## Context
The repo carries guidance for Claude Code in two forms: `CLAUDE.md` files and
`.claude/skills/`. Rules copied into several places drift; a naming rule
restated in three skills ends up with three versions.

## Decision
`CLAUDE.md` (root and per tool) holds conventions: what is true about the code.
Skills hold procedures: the steps to do a task, the files that move together,
the command that proves it worked, and the mistakes actually hit. A skill
references the convention it depends on and never restates it. Toolkit skills
work on this repo; stack skills work in a downstream project and must not
assume this repo's layout. Rules for writing skills: `.claude/skills/CLAUDE.md`.

## Consequences
Each rule has one home, so it changes in one place. A skill can go stale if
the procedure changes (several did when the fixture seeds landed), so a PR that
changes a procedure updates the skills that describe it.
