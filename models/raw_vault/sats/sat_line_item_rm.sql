{%- set source_model = "v_stg_phases" -%}
{%- set src_pk = "LINE_ITEM_HASH_KEY" -%}
{%- set src_hashdiff = "HASHDIFF" -%}
{%- set src_payload = {"exclude_columns": "true", "columns": ["LINE_ITEM_BUSINESS_KEY","PARENT_ID","DEAL_HASH_KEY","HUBSPOT_DEAL_FK",
                        "HUBSPOT_DEAL_FK_HASH_KEY","HUBSPOT_LINE_ITEM_FK","HUBSPOT_LINE_ITEM_FK_HASH_KEY","PROJECT_CODE_HASH_KEY",
                        "LINK_LINE_ITEM_DEAL_HASH_KEY","LINK_LINE_ITEM_HASH_KEY","_FIVETRAN_SYNCED"]} -%}
{%- set src_ldts = "LOAD_DATE_TIMESTAMP" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}