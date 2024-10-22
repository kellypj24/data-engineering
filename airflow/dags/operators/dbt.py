import json
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.utils.log.secrets_masker import mask_secret
from operators.postgres_connection import get_postgres_secrets


class Dbt(BashOperator):
    @apply_defaults
    def __init__(self, command, *args, **kwargs):
        # Get connection details
        conn_params = get_postgres_secrets()

        # Mask secrets in the UI and in the logs
        for key, value in conn_params.items():
            mask_secret(value)

        # Set up environment variables for dbt
        env_vars = f"""
        export PG_DATABASE='{conn_params["dbname"]}';
        export PG_HOST='{conn_params["host"]}';
        export PG_PASSWORD='{conn_params["password"]}';
        export PG_PORT='{conn_params["port"]}';
        export PG_USER='{conn_params["username"]}';
        """

        # Construct the bash command
        bash_command = f"""
        {env_vars}
        echo "Current PATH: $PATH"
        echo "Attempting to install dbt-core and dbt-postgres:"
        pip install --user dbt-core dbt-postgres
        export PATH=$PATH:$HOME/.local/bin
        echo "Updated PATH: $PATH"
        echo "Attempting to find dbt:"
        command -v dbt || {{ echo "dbt not found in PATH" >&2; exit 1; }}
        echo "DBT version:"
        dbt --version
        echo "Running dbt {command}:"
        dbt {command} --project-dir /tmp/dbt --profiles-dir /tmp/dbt --target prod || {{ echo "dbt {command} failed" >&2; exit 1; }}
        """

        super().__init__(bash_command=bash_command, *args, **kwargs)
        self.command = command
