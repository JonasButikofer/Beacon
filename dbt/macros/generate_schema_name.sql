{#- Use +schema as-is (gold, silver) instead of dbt's default <target>_<schema>,
    so seeds/marts land in beacon.gold rather than beacon.silver_gold. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}
