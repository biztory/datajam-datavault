{%- set yaml_metadata -%}
source_model:
  google_sheets: biztory_team
derived_columns:
  STAFF_BUSINESS_KEY: SPLIT_PART(EMAIL_ADDRESS,'@',1)
  EMAIL_USERNAME_FK: 'NULL'
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!GSHEETS'
  RECORD_SOURCE: '!{{ source('google_sheets', 'biztory_team') }}'
  DBTVAULT_RANK: "RANK() OVER(PARTITION BY STAFF_BUSINESS_KEY ORDER BY LOAD_DATE_TIMESTAMP)"
hashed_columns:
  STAFF_HASH_KEY:
    - STAFF_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  LINK_STAFF_HASH_KEY:
    - STAFF_BUSINESS_KEY
    - EMAIL_USERNAME_FK
    - BUSINESS_KEY_COLLISION_CODE
  EMAIL_USERNAME_HASH_KEY:
    - STAFF_BUSINESS_KEY
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - STAFF_HASH_KEY
      - STAFF_BUSINESS_KEY
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

