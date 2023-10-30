Datascience Dagster
===================

The `main` github branch will deploy to the `main` Dagster [code location](https://docs.dagster.io/concepts/code-locations#code-locations) here https://company.dagster.cloud/prod/locations
It references the `company` package, so new jobs, assets, schedules etc. should be added to the 
[`Definitions`](https://docs.dagster.io/_apidocs/definitions#dagster.Definitions)
declaration in [company/__init__.py](company/__init__.py).

Any merge to `main` will redeploy the `main` Dagster code location.
A Github action builds and pushes a new Docker image, tagged with the git commit SHA,
then deploys this to Dagster.

If you git tag a branch in `datascience-dagster` with `deploy-<name>` (e.g. `deploy-foobar`),
this will build a new docker image tagged `262456549263.dkr.ecr.us-west-2.amazonaws.com/datascience-dagster:deploy-foobar`.
This can then be provisioned as a new code location in the Dagster cloud UI for testing branches.
These jobs will run against the same live Redshift database as the `main` code location.

## Development

### Database Access
Set `PGUSER` and `PGPASSWORD` environment variables to your Redshift credentials and run
`bin/docker-dagster.sh` to launch the [`dagit`](https://docs.dagster.io/concepts/dagit/dagit#dagit-ui)
webUI (similar to Dagster Cloud UI) - you can then access it in your browser via
http://localhost:3000 and execute your jobs etc. locally.

If you set `PGHOST=playbastion-public.play.internal.aws.company.com` before launching, you can  use play Redshift instead, you can also set `PGDATABASE` to point to a play database.
You must be using the company VPN to access play or live Redshift.

### Google Drive/BigQuery Access
The [google_drive_io_manager](company/io_managers/google_drive_io_manager.py) uses authentication information stored in an AWS secret share facilitate dropping files into company's workspace.
To test locally add a file named `GOOGLE_CREDENTIALS.json` in the `.local` directory. 

The contents should look similar to this:
```json
{
    "type": "service_account",
    "project_id": "<OMITTED>",
    "private_key_id": "<OMITTED>",
    "private_key": "<OMITTED>",
    "client_email": "<OMITTED>",
    "client_id": "<OMITTED>",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "<OMITTED>",
    "universe_domain": "googleapis.com"
}
```

:exclamation: do not commit this file into git history :exclamation:
:exclamation: you will leak sensitive credentials :exclamation:

Here is an example launching dagit against a personal database in play Redshift, note that the password is securely read into an environment variable to avoid exposing it via the shell history:

```sh-session
[macos]$ read -s -p "Password: " PGPASSWORD
[macos]$ export PGUSER=rectalogic PGPASSWORD PGHOST=playbastion-public.play.internal.aws.company.com
[macos]$ bin/docker-dagster.sh
dev: Pulling from company/datascience-dagster-base
Digest: sha256:64fa18d7d411851e574361393bef0a40aa3008dddb58e7af3e2f0aa836cb8c43
Status: Image is up to date for public.ecr.aws/company/datascience-dagster-base:dev
2023-04-06 18:36:49 +0000 - dagster - INFO - Launching Dagster services...
2023-04-06 18:36:53 +0000 - dagster.daemon - INFO - Instance is configured with the following daemons: ['BackfillDaemon', 'SchedulerDaemon', 'SensorDaemon']
2023-04-06 18:36:53 +0000 - dagster.daemon.SensorDaemon - INFO - Not checking for any runs since no sensors have been started.
2023-04-06 18:36:53 +0000 - dagit - INFO - Serving dagit on http://0.0.0.0:3000 in process 9
```
Now load http://localhost:3000 in your browser, from there you can launch runs etc.

Linting is performed by a Github Action - you can reformat your code to the black standard using `bin/docker-shell.sh bin/reformat.sh`.
You can run lint locally to validate your code with `bin/docker-shell.sh bin/lint.sh`.
