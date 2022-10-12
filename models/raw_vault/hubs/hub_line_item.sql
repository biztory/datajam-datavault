{%- set source_model = ["v_stg_line_item","v_stg_phases"] -%}
{%- set src_pk = "LINE_ITEM_HASH_KEY" -%}
{%- set src_nk = "LINE_ITEM_BUSINESS_KEY" -%}
{%- set src_ldts = "LOAD_DATE_TIMESTAMP" -%}
{%- set src_source = "RECORD_SOURCE" -%}
{%- set src_extra_columns = ["BUSINESS_KEY_COLLISION_CODE","PROJECT_CODE_HASH_KEY"] -%} --Should be renamed to add FK to name?

{{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model, src_extra_columns=src_extra_columns) }}