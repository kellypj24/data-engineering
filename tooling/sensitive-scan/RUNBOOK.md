# Sensitive data in git history: incident runbook

Once a value is pushed, assume it is copied: clones, forks, CI caches, and
mirrors all hold it. Removing it from history limits further spread; it does
not undo the exposure. Work in this order.

1. **Contain.** Stop further pushes to the affected branch. If the repository is
   public, make it private while you work, if policy allows.
2. **Rotate first, for secrets.** Revoke and reissue the key, token, or
   password **before** rewriting history. A rewritten history with a live key
   is still a leak. For PII there is nothing to rotate; go to step 5 in
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
4. **Force-push and invalidate.**
   - `git push --force --mirror`.
   - Ask the host to purge cached views and dangling commits (GitHub: contact
     support with the commit SHAs; pull-request refs keep old commits alive).
   - Delete CI caches and artifacts built from the affected commits.
   - Every collaborator must re-clone. A stale clone pushed later restores the
     data.
5. **Notify.** Follow your data-protection process: who was affected, what
   data, for how long, who could access it. Regulated data (health, financial,
   personal) usually has a reporting deadline measured in hours or days.
6. **Prevent.** Add the case as a fixture test in `tests/test_scan.py` if the
   scanner missed it, and make the CI step a required check.
