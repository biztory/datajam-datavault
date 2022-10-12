{%- set yaml_metadata -%}
source_model:
  smartsheet: smartsheet_request_form_responses
derived_columns:
  RESPONSE_BUSINESS_KEY: '_ROW'
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!SMARTSHEET'
  RECORD_SOURCE: '!{{ source('smartsheet', 'smartsheet_request_form_responses') }}'
  DBTVAULT_RANK: "RANK() OVER(PARTITION BY RESPONSE_BUSINESS_KEY ORDER BY LOAD_DATE_TIMESTAMP)"
hashed_columns:
  RESPONSE_HASH_KEY:
    - RESPONSE_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  PROJECT_CODE_HASH_KEY:
    - HUB_SPOT_LINE_ITEM
    - BUSINESS_KEY_COLLISION_CODE
  LINK_RESPONSE_LINE_ITEM_HASH_KEY:
    - RESPONSE_BUSINESS_KEY
    - HUB_SPOT_LINE_ITEM
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - RESPONSE_HASH_KEY
      - RESPONSE_BUSINESS_KEY
      - PROJECT_CODE_HASH_KEY
      - LINK_RESPONSE_LINE_ITEM_HASH_KEY
      - LOAD_DATE_TIMESTAMP
      - BUSINESS_KEY_COLLISION_CODE
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ dbtvault.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=none) }}
