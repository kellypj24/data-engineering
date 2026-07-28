---
name: add-tool
description: Use when adding a new tool to this toolkit — "add a new orchestrator", "add Flyte/Mage/Meltano/SQLMesh", "scaffold a new EL tool", "add a transformation tool". Creates the tool directory and wires it into the justfile, CI, dependabot, and the README table. Not for using a tool inside a downstream project.
argument-hint: "<role> <tool-name>   e.g. orchestration flyte"
---

# Add a tool to the toolkit

A tool is not "added" when its directory exists. It is added when it is wired
into all five shared surfaces below. Missing one is the failure mode this skill
exists to prevent — `transformation/dbt` sat for months with no `pyproject.toml`,
no dependabot entry, and no CI job, and nothing caught it.

Conventions (uv, just, per-tool independence, mocked services in tests) live in
the root `CLAUDE.md`. Read it first; this skill does not restate it.

## 1. Read the role's requirements

`<role>/_template/README.md` states what a tool of that role must provide. It is
a **specification, not a skeleton** — there are no files to copy. Read it as a
checklist of capabilities the new tool has to cover.

Roles: `extract_load/`, `orchestration/`, `transformation/`.

## 2. Create the tool directory

Model it on `extract_load/dlt/` — the smallest complete tool in the repo.

```
<role>/<tool-name>/
├── pyproject.toml     # [project] + [dependency-groups] dev; see a sibling tool
├── uv.lock            # generated -- never hand-edited
├── README.md          # what it is, when to choose it, how to run it
├── CLAUDE.md          # key files, commands, patterns -- for agents
├── mod.just           # test / lint / fmt at minimum
└── tests/             # pytest, mocked services only
```

`mod.just` recipe bodies run **in the tool's own directory** — `just` sets that
automatically. Do not prefix them with `cd`. If a recipe needs a subdirectory,
use a relative path or `source_directory()`.

Generate the lock immediately: `cd <role>/<tool-name> && uv lock`.

## 3. Wire it into the five shared surfaces

All five, or the tool is invisible to some part of the system.

1. **Root `justfile`** — add `mod <tool-name> '<role>/<tool-name>'`, and add the
   tool to the aggregate `test`, `lint`, and `fmt` recipes.
2. **`.github/workflows/ci.yml`** — add a `paths-filter` entry under
   `detect-changes`, a `test-<tool>` job gated on it, and a row in the `lint`
   matrix. If the tool participates in cross-paradigm impact (e.g. a transform
   tool that orchestrators wrap), update `resolve-impacts` too.
3. **`.github/dependabot.yml`** — add a `package-ecosystem: uv` entry pointing at
   the tool directory. Use `uv`, never `pip`: the `pip` ecosystem updates
   `pyproject.toml` but leaves `uv.lock` untouched, and the `lockfiles` CI job
   will fail on the resulting drift.
4. **Root `README.md`** — add a row to the tool table (`| Role | Tool | Description | Status |`).
5. **Root `CLAUDE.md`** — only if the tool introduces a new convention. Usually
   it does not.

## 4. Verify

Every one of these must pass before you call it done:

```bash
just --list                      # the module loads and its recipes appear
just <tool-name>::test           # runs in the tool's directory, not the repo root
just <tool-name>::lint
cd <role>/<tool-name> && uv lock --check   # lock matches pyproject
just test && just lint           # aggregates still green
```

Then confirm CI would actually exercise it: the paths-filter entry must match
the directory you created, or the job silently skips forever.

## Common mistakes

- **Prefixing `mod.just` recipes with `cd {{justfile_directory()}}`.** Inside a
  module that resolves to the *repo root*, not the tool directory, so every
  recipe runs in the wrong place. Every tool in this repo had this bug.
- **Adding the paths-filter but not the job**, or a filter path that does not
  match the real directory. The job then skips on every PR and CI stays green
  while the tool is untested. Check an actual PR's job list, not just the YAML.
- **Using `package-ecosystem: pip`.** See step 3.3.
- **Forgetting `uv lock`.** Without a committed lock the `lockfiles` job fails.
- **Tests that need a live service.** Mock them — see how the existing tools do
  it. CI has no warehouse, no broker, no cluster.
- **Stopping at the directory.** The tool exists but nothing lints, tests,
  updates, or documents it. That is the exact state dbt was in.
