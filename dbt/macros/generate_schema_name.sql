-- Override dbt's default schema-name generator.
--
-- Default behavior: target.schema + "_" + +schema (e.g. "dbt_default_gold")
-- Our behavior:    use +schema literally; fall back to target.schema if unset.
--
-- This keeps schemas clean and predictable:
--   models/staging/* → dbt_staging  (per +schema in dbt_project.yml)
--   models/gold/*    → gold         (per +schema in dbt_project.yml)
--
-- Reference: https://docs.getdbt.com/docs/build/custom-schemas#changing-the-way-dbt-generates-a-schema-name

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
