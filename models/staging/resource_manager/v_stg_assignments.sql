{%- set yaml_metadata -%}
source_model:
  resource_manager: assignments
derived_columns:
  ASSIGNMENT_BUSINESS_KEY: ID
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!RESOURCE_MANAGER'
  RECORD_SOURCE: '!{{ source('resource_manager', 'assignments') }}'
  DBTVAULT_RANK: "RANK() OVER(PARTITION BY ASSIGNMENT_BUSINESS_KEY ORDER BY LOAD_DATE_TIMESTAMP)"
hashed_columns:
  ASSIGNMENT_HASH_KEY:
    - ASSIGNMENT_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  LINE_ITEM_HASH_KEY:
    - ASSIGNABLE_ID
    - BUSINESS_KEY_COLLISION_CODE
  STAFF_HASH_KEY:
    - USER_ID
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - ASSIGNMENT_HASH_KEY
      - ASSIGNMENT_BUSINESS_KEY
      - LINE_ITEM_HASH_KEY
      - STAFF_HASH_KEY
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
