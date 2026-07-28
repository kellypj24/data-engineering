---
name: add-stack
description: Use when composing existing toolkit tools into a new pre-assembled stack — "add a stack for X + Y + Z", "create a new stack", "combine dlt and airflow and dbt". Creates stacks/<name>/ with its README, compose file, and wiring notes. Not for adding a new tool (use add-tool) or for building inside a downstream project.
argument-hint: "<stack-name>   e.g. dlt-airflow-dbt"
---

# Add a stack

A stack is a **documented composition** of tools that already exist in this
repo — not new code. Its job is to answer "when would I choose this, and how do
the pieces connect?" for someone deciding what to adopt.

Name it for its components in pipeline order: `<el>-<orchestrator>-<transform>`,
e.g. `dlt-dagster-dbt`.

## 1. Check the components exist and are healthy

Every tool named in the stack must already exist under `extract_load/`,
`orchestration/`, or `transformation/`, and be `Ready` in the root README's tool
table. Run the `verify-tool` skill on each before composing them — a stack built
on a broken tool documents a path that does not work.

If a component does not exist yet, use `add-tool` first, in its own PR.

## 2. Read the stack specification

`stacks/_template/README.md` states what a stack must document. As with the tool
templates, it is a **specification, not a skeleton**.

## 3. Create the stack directory

```
stacks/<stack-name>/
├── README.md            # required
└── docker-compose.yml   # only if the stack needs services to run locally
```

Model the README on `stacks/dlt-dagster-dbt/` — the clearest existing example.
It must cover:

- **When to use this stack** — the decision criteria, stated against the
  alternatives. This is the section people actually read; write it as "choose
  this over `<other-stack>` when…".
- **Architecture overview** — an ASCII diagram of the data flow.
- **How the pieces connect** — which tool calls which, and where the handoff
  happens (e.g. dlt embedded in Dagster assets vs. Airbyte triggered as a
  separate service).
- **Setup** — numbered steps, pointing at the component directories with
  relative links rather than duplicating their instructions.
- **Related documentation** — relative links to each component's README.

Add `docker-compose.yml` only when the stack genuinely needs long-running
services. `stacks/airbyte-dagster-dbt/` has one because Airbyte is a service;
`dlt-dagster-dbt` has none because dlt is a library.

## 4. Wire it in

- **Root `README.md`** — add a row to the stacks table
  (`| Stack | Components | Use Case |`).
- Stacks need **no** `justfile`, CI, or dependabot entries. They contain no code
  of their own — the components are already covered. Do not add jobs for them.

## 5. Verify

```bash
# every relative link in the new README resolves
grep -o '](\.\./[^)]*)' stacks/<stack-name>/README.md

# compose file parses, if you added one
docker compose -f stacks/<stack-name>/docker-compose.yml config >/dev/null
```

Then confirm the root README table renders and its links resolve.

## Common mistakes

- **Duplicating component setup instructions.** They drift immediately. Link to
  the component README instead — that is where the instructions are maintained.
- **Adding CI jobs for the stack.** There is no code to test. The components
  already have jobs; a stack job would test nothing and skip forever.
- **Broken relative links.** Stack READMEs sit two levels down, so component
  links are `../../<role>/<tool>/README.md`. Getting this wrong is the most
  common defect in these files.
- **Writing "when to use" as a feature list.** It should be a decision rule
  against the other stacks, or it does not help anyone choose.
- **Composing a tool that is not `Ready`.** Check the root README's Status
  column before including it.
