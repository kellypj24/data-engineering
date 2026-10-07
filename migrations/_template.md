# NNNN — <what changes>

Copy to `migrations/NNNN-short-title.md` in the PR that needs it. Numbered in
merge order. One file per PR, covering every environment.

- **PR:** #
- **Why code promotion is not enough:** <the physical state the deploy does not change>

## Per environment

| Environment | Action | Who runs it | When | Done |
|-------------|--------|-------------|------|------|
| dev (personal) | | each engineer | after pulling | n/a |
| stage | | | before / after the stage deploy | [ ] |
| prod | | | before / after the prod deploy | [ ] |

Action is one of: none; reload seed `<name>`; rebuild `<selector>` with
`--full-refresh` (only if not a durable fact); backfill `<model>` for
`<window>` (the `backfill-runbook` skill); run `<run-operation>`; manual DDL
(write it out in full below).

## Commands

```bash
# exact commands, per environment
```

## Verify

```bash
# a query or command whose result proves it is done, per environment
```

## Rollback

<how to undo it, or why it cannot be undone and what was backed up first>
