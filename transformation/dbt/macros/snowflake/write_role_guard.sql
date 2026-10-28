{#-
    Snowflake extra. Pre-run write-role guard, wired as an `on-run-start` hook
    in dbt_project.yml:

        on-run-start:
          - "{{ assert_write_role() }}"

    Why: a developer who runs dbt against a shared environment under a
    personal role creates objects that role owns. The environment's owner role
    then cannot replace them: the drift `audit_object_ownership` reports and
    `normalize_object_ownership` repairs after the fact.

    For `run`, `build`, `seed`, and `snapshot` against a shared database (a
    value of the `environment_databases` var), CURRENT_ROLE() must be
    <DATABASE>_OWNER, the owner role the ownership audit expects, or one the
    `write_role_allowlist` var names for that database (or for "*"):

        write_role_allowlist:
          ANALYTICS_STAGE: [ANALYTICS_STAGE_BREAKGLASS]
          "*": [SYSADMIN]

    Every other command, a personal (E31) database, and any adapter but
    Snowflake are skipped. The guard checks `target.database`, not models
    configured with their own `database`.

    Caveat: dbt runs `create schema if not exists` before `on-run-start`
    fires, so a wrong role can still create an empty schema. The guard stops
    table and view writes, not that.
-#}

{#- The rule. Returns {verdict: pass|fail|skip, reason}. Pure, so it is
    tested with literal inputs on any adapter. -#}
{% macro write_role_check(database, role, command, shared_databases, allowlist) %}
    {%- set database = (database or '') | upper -%}
    {%- set role = (role or '') | upper -%}
    {%- if command not in ['run', 'build', 'seed', 'snapshot'] -%}
        {{ return({'verdict': 'skip', 'reason': command ~ ' does not write'}) }}
    {%- endif -%}
    {%- if database not in shared_databases | map('upper') | list -%}
        {{ return({'verdict': 'skip', 'reason': database ~ ' is not a shared environment database'}) }}
    {%- endif -%}
    {%- set permitted = [database ~ '_OWNER'] -%}
    {%- for key, roles in (allowlist or {}).items() if key | upper in [database, '*'] -%}
        {%- do permitted.extend(roles | map('upper')) -%}
    {%- endfor -%}
    {%- if role in permitted -%}
        {{ return({'verdict': 'pass', 'reason': role ~ ' may write to ' ~ database}) }}
    {%- endif -%}
    {{ return({'verdict': 'fail', 'reason':
        command ~ ' against ' ~ database ~ ' as ' ~ role ~ '; permitted: ' ~ permitted | join(', ')
        ~ '. Objects this role creates would drift from ' ~ database ~ '_OWNER (audit_object_ownership).'}) }}
{% endmacro %}

{% macro assert_write_role() %}
    {%- if not execute or adapter.type() != 'snowflake' -%}
        {{ return('') }}
    {%- endif -%}
    {%- set check = write_role_check(
        target.database,
        run_query('SELECT CURRENT_ROLE()').columns[0].values()[0],
        flags.WHICH,
        var('environment_databases', {}).values() | list,
        var('write_role_allowlist', {}),
    ) -%}
    {%- if check.verdict == 'fail' -%}
        {{ exceptions.raise_compiler_error('assert_write_role: ' ~ check.reason) }}
    {%- endif -%}
    {{ log('assert_write_role: ' ~ check.verdict ~ ', ' ~ check.reason, info=check.verdict == 'pass') }}
    {{ return('') }}
{% endmacro %}
