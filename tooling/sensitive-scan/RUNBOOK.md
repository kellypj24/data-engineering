# Sensitive data in git history: incident runbook

## Not yet pushed

If the value is only in local commits, fix it locally and stop here: no
remote history is rewritten.

```bash
git reset --soft HEAD~1         # last commit: unstage, remove the value, recommit
git commit --amend              # or amend after removing it
git rebase -i <first-bad>^      # older local commits: edit each one
sensitive-scan --all            # nothing left before you push
```

Check `git branch -r --contains <sha>` first: if any remote branch contains
the commit, it is pushed. Use the rest of this runbook.

## Pushed

Once a value is pushed, assume it is copied: clones, forks, CI caches, and
mirrors all hold it. Removing it from history limits further spread; it does
not undo the exposure. Work in this order.

1. **Contain and freeze.** Stop merges and pushes to the repository before
   rewriting anything: announce the freeze, pause merge queues and bots that
   push (Dependabot, release automation). A push that lands mid-rewrite is
   either lost or brings the value back. If the repository is public, make it
   private while you work, if policy allows.
2. **Rotate first, for secrets.** Revoke and reissue the key, token, or
   password **before** rewriting history. A rewritten history with a live key
   is still a leak. For PII there is nothing to rotate; go to step 7 in
   parallel.
3. **Rewrite history.** Remove the file or value from every commit:

   ```bash
   # In a fresh mirror clone
   git clone --mirror git@host:org/repo.git
   cd repo.git
   git filter-repo --invert-paths --path path/to/file      # drop a file
   git filter-repo --replace-text expressions.txt          # or scrub values
   ```

   `expressions.txt` lists one literal per line (`literal==>REDACTED`).
4. **Verify before force-pushing.** In the rewritten mirror, every ref must be
   clean:

   ```bash
   git log -p --all | grep -c -F -f ../literals.txt  # the gate: expect 0
   git log -p --all > ../history.patch
   sensitive-scan ../history.patch                   # second net: the same kind of value elsewhere
   ```

   `literals.txt` holds the raw values, one per line. Keep it and the patch
   outside the repository and delete both when the incident closes. Do not
   push until the `grep` count is 0. Scanner findings on known synthetic
   fixtures are expected; anything else is a value you have not scrubbed yet.
5. **Lift branch protection, force-push, restore it.** Record each step with
   the time and who did it, in the incident record:
   - lift force-push protection on the affected branches only;
   - `git push --force --mirror`;
   - restore the protection exactly as it was, and check it (GitHub:
     `gh api repos/<org>/<repo>/rulesets`).
6. **Audit scrub completeness, and invalidate.**
   - Clone fresh and repeat step 4's checks over **every branch and tag**.
   - Check every **open pull request's** refs (`refs/pull/*/head`): the host
     keeps them, and an open PR still serves the old commits. Close and
     recreate those PRs from the rewritten branches.
   - Ask the host to purge cached views and dangling commits (GitHub: contact
     support with the commit SHAs).
   - Delete CI caches and artifacts built from the affected commits.
   - Every collaborator must re-clone. A stale clone pushed later restores the
     data. Lift the freeze only after this.
7. **Notify.** Follow your data-protection process: who was affected, what
   data, for how long, who could access it. Regulated data (health, financial,
   personal) usually has a reporting deadline measured in hours or days.
8. **Prevent.** Add the case as a fixture test in `tests/test_scan.py` if the
   scanner missed it, and make the CI step a required check.
