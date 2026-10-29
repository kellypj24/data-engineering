# Image promotion: the image that soaked in stage is the image in prod

For deployable images: a Dagster code location, a dbt runner, the
file-export CLI. Branch promotion paths (`dev → staging → main`) are out of
scope; see *Context-dependent — deliberately NOT recommended (yet)* in
`docs/ci-cd-hardening.md`.

## Rules

| Rule | Why |
|---|---|
| Build once, on merge to main; tag with the commit SHA and record the digest | Prod runs bytes that were tested, not a rebuild that resolved dependencies again |
| Deploy and promote by **digest** (`image@sha256:...`) | Tags move. `latest` is whatever was pushed last, and a re-pushed SHA tag is a different image |
| Stage first, then **soak** for at least one full cycle of the schedules that matter (24h for daily jobs; past month end for period-close jobs) | Most breakage shows only on a scheduled run with real data: a sensor cursor, a timezone, a month-end branch |
| Promote outside **quiet windows**: not while a critical job is running, not during period close, not before a weekend or holiday without cover | A failed promotion during a run leaves that run half on each version, with nobody watching |
| Roll back to the **previous digest**, recorded at promotion time | Rollback is a redeploy of a known image, not a revert, rebuild, and wait for CI |
| Keep images long enough to roll back: at least the current and previous prod digests, and everything younger than the longest soak | A registry lifecycle rule that deletes by age can delete the rollback target |

## Promote

1. Note the current prod digest. That is the rollback target.
2. Confirm the stage digest has soaked: the deploy time is older than the soak
   period, and the scheduled runs since then succeeded
   (`tooling/dagster-status`: `recent`, `errors`).
3. Check the quiet window: `dagster-status status` shows no critical run in
   progress.
4. Deploy the stage digest to prod.
5. Watch the first scheduled run of each critical job. Record the promotion
   (digest, time, who) where the team keeps deploy history.

## Roll back

1. Deploy the previous digest recorded in step 1.
2. Confirm the next scheduled runs succeed.
3. Keep the bad digest out of stage until a fix has soaked.

A rollback reverts code, not data. If the bad image wrote tables, repair them
as a [state migration](state-migrations.md).

## Registry retention

Exempt the current and previous prod digests, and the current stage digest,
from any lifecycle rule. Delete by age only beyond the longest soak period.
