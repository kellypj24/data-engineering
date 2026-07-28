# Writing skills for this repo

## The cardinal rule

**Skills are procedure. `CLAUDE.md` is convention.**

A skill says *how to carry out a task here* — the order of steps, the files that
must move together, the check that proves it worked. It must **reference**
conventions rather than restate them. Three copies of a style guide drift, and
the linters outlive all of them.

- Convention ("we use `uv`, never `pip`") → root `CLAUDE.md` or the tool's own.
- Procedure ("adding a tool touches these six files, in this order") → a skill.

If you catch yourself pasting a naming rule or a dependency policy into a skill,
delete it and link to the `CLAUDE.md` that owns it.

## Two families

**Toolkit skills** — for working *on* this repo: adding a tool, adding a stack,
verifying one is healthy. They know about the root `justfile`, `ci.yml`'s
paths-filter, `dependabot.yml`, and the README tables.

**Stack skills** — for working *with* a stack once it has been chosen and copied
into a real project: adding a dbt model, a Dagster asset, a dlt pipeline. These
must **not** assume this repo's directory layout, because they run in the
downstream project. Take the tool's root as an argument or infer it.

Keep the two separate. A skill that does both ends up wrong in both contexts.

## File layout

```
.claude/skills/<kebab-case-name>/SKILL.md
```

Frontmatter:

```yaml
---
name: add-tool
description: Use when adding a new tool to the toolkit — ...
argument-hint: "<role> <tool-name>"
---
```

`description` is the **only** thing the model matches on when deciding whether to
invoke a skill. Write it as a trigger ("Use when the user asks to…"), name the
concrete nouns someone would actually say, and keep it specific enough that it
does not fire on unrelated work.

## What every skill must end with

1. **Verify.** A command whose exit code proves the work landed —
   `just --list`, `just <tool>::test`, `uv lock --check`. Not "check that it
   looks right."
2. **Common mistakes.** The failures actually hit in this repo, not imagined
   ones. This section is the highest-value part of the file; prefer a real
   footgun over a generic caution.

## Grounding

Prefer describing a real file to inventing an example. `extract_load/dlt/` is the
smallest complete tool and is the best model for what "finished" looks like.

Note that `<role>/_template/` is a **specification document** — a README stating
what a tool of that role must provide. It is not a code skeleton to copy. Read it
as the requirements checklist, then generate the files.
