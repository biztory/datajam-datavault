{%- set source_model = "v_stg_operations_resource_request_form" -%}
{%- set src_pk = "RESPONSE_HASH_KEY" -%}
{%- set src_hashdiff = "HASHDIFF" -%}
{%- set src_payload = {"exclude_columns": "true", "columns": ["RESPONSE_BUSINESS_KEY","PROJECT_CODE_HASH_KEY","HUB_SPOT_LINE_ITEM","LINK_RESPONSE_LINE_ITEM_HASH_KEY","_FIVETRAN_SYNCED"]} -%}
{%- set src_ldts = "LOAD_DATE_TIMESTAMP" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}