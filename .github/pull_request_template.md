## Summary

<!-- What changed and why, in 1-3 bullets. Title: `type(scope): subject`. -->

-

**Tools touched:**

## How tested

<!-- The commands you ran locally and what they printed. CI is the second check, not the first. -->

-

## Checklist

- [ ] Gauntlet green locally for every tool touched (`just <tool>::test`, `::lint`, `::fmt-check`)
- [ ] Docs updated: the tool's README / CLAUDE.md, and the root README table if a tool or stack was added
- [ ] Roadmap box ticked if this closes a task in `docs/toolkit-expansion.md` or `docs/ci-cd-hardening.md`
- [ ] **State migration needed?** If this changes physical state that promoting code does not carry (a seed reload, a rebuild, an incremental table's columns, manual DDL), add `migrations/NNNN-<title>.md` from `migrations/_template.md`. See `docs/patterns/state-migrations.md`. Otherwise leave this unticked.
