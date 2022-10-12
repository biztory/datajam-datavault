{%- set yaml_metadata -%}
source_model:
  hubspot: owner
derived_columns:
  STAFF_BUSINESS_KEY: OWNER_ID::VARCHAR
  EMAIL_USERNAME_FK: SPLIT_PART(EMAIL,'@',1)
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!HUBSPOT'
  RECORD_SOURCE: '!{{ source('hubspot', 'owner') }}'
  DBTVAULT_RANK: "RANK() OVER(PARTITION BY STAFF_BUSINESS_KEY ORDER BY LOAD_DATE_TIMESTAMP)"
hashed_columns:
  STAFF_HASH_KEY:
    - STAFF_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  EMAIL_USERNAME_HASH_KEY:
    - EMAIL_USERNAME_FK
  LINK_STAFF_HASH_KEY:
    - STAFF_BUSINESS_KEY
    - EMAIL_USERNAME_FK
    - BUSINESS_KEY_COLLISION_CODE
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - STAFF_HASH_KEY
      - STAFF_BUSINESS_KEY
      - EMAIL_USERNAME_FK_HASH_KEY
      - LINK_STAFF_HASH_KEY
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
