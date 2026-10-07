{#-
    Enforce the tagging contract (docs/patterns/tagging.md):

        dbt run-operation check_tag_contract

    For every enabled model: exactly one tag from tag_taxonomy.layer (set by
    directory in dbt_project.yml), and no tag outside tag_taxonomy. Also
    rejects a taxonomy value listed under two axes. Lists every violation, then
    fails. Without enforcement a taxonomy decays into tag soup within months.
-#}

{% macro check_tag_contract() %}
    {%- if not execute -%}
        {{ return(none) }}
    {%- endif -%}
    {%- set taxonomy = var('tag_taxonomy') -%}
    {%- set layers = taxonomy.get('layer', []) -%}
    {%- set allowed = [] -%}
    {%- set problems = [] -%}

    {%- for axis, values in taxonomy.items() -%}
        {%- for value in values -%}
            {%- if value in allowed -%}
                {%- do problems.append("tag_taxonomy: '" ~ value ~ "' is listed under more than one axis") -%}
            {%- endif -%}
            {%- do allowed.append(value) -%}
        {%- endfor -%}
    {%- endfor -%}

    {%- set checked = [] -%}
    {%- for node in graph.nodes.values() if node.resource_type == 'model' -%}
        {%- do checked.append(node.name) -%}
        {%- set layer_tags = node.tags | select('in', layers) | list -%}
        {%- if layer_tags | length != 1 -%}
            {%- do problems.append(node.name ~ ": needs exactly one layer tag, has " ~ (layer_tags | tojson)) -%}
        {%- endif -%}
        {%- for tag in node.tags if tag not in allowed -%}
            {%- do problems.append(node.name ~ ": tag '" ~ tag ~ "' is not in tag_taxonomy") -%}
        {%- endfor -%}
    {%- endfor -%}

    {%- for problem in problems -%}
        {{ log("check_tag_contract: " ~ problem, info=true) }}
    {%- endfor -%}
    {%- if problems -%}
        {{ exceptions.raise_compiler_error("check_tag_contract: " ~ problems | length ~ " violation(s)") }}
    {%- endif -%}
    {{ log("check_tag_contract: " ~ checked | length ~ " models, all compliant", info=true) }}
{% endmacro %}
