{%- set yaml_metadata -%}
source_model:
  resource_manager: projects_phases
derived_columns:
  LINE_ITEM_BUSINESS_KEY: ID
  HUBSPOT_DEAL_FK: SPLIT_PART(DESCRIPTION, ' | ',1)
  HUBSPOT_LINE_ITEM_FK: SPLIT_PART(DESCRIPTION, ' | ',2)
  LOAD_DATE_TIMESTAMP: '_FIVETRAN_SYNCED'
  BUSINESS_KEY_COLLISION_CODE: '!RESOURCE_MANAGER'
  RECORD_SOURCE: '!{{ source('resource_manager', 'projects_phases') }}'
  DBTVAULT_RANK: "RANK() OVER(PARTITION BY LINE_ITEM_BUSINESS_KEY ORDER BY LOAD_DATE_TIMESTAMP)"
hashed_columns:
  LINE_ITEM_HASH_KEY:
    - LINE_ITEM_BUSINESS_KEY
    - BUSINESS_KEY_COLLISION_CODE
  DEAL_HASH_KEY:
    - PARENT_ID
    - BUSINESS_KEY_COLLISION_CODE
  HUBSPOT_DEAL_FK_HASH_KEY:
    - HUBSPOT_DEAL_FK
    - BUSINESS_KEY_COLLISION_CODE  
  HUBSPOT_LINE_ITEM_FK_HASH_KEY:
    - HUBSPOT_LINE_ITEM_FK
    - BUSINESS_KEY_COLLISION_CODE 
  PROJECT_CODE_HASH_KEY:
    - PROJECT_CODE
    - BUSINESS_KEY_COLLISION_CODE 
  LINK_LINE_ITEM_DEAL_HASH_KEY:
    - LINE_ITEM_BUSINESS_KEY
    - PARENT_ID
    - BUSINESS_KEY_COLLISION_CODE
  LINK_LINE_ITEM_HASH_KEY:
    - LINE_ITEM_BUSINESS_KEY
    - HUBSPOT_LINE_ITEM_FK
    - BUSINESS_KEY_COLLISION_CODE
  SAL_LINE_ITEM_HASH_KEY:
    - HUBSPOT_LINE_ITEM_FK
  HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - LINE_ITEM_HASH_KEY
      - LINE_ITEM_BUSINESS_KEY
      - DEAL_HASH_KEY
      - HUBSPOT_DEAL_FK_HASH_KEY
      - HUBSPOT_LINE_ITEM_FK_HASH_KEY
      - PROJECT_CODE_HASH_KEY
      - LINK_LINE_ITEM_DEAL_HASH_KEY
      - LINK_LINE_ITEM_HASH_KEY
      - SAL_LINE_ITEM_HASH_KEY
      - LOAD_DATE_TIMESTAMP
      - BUSINESS_KEY_COLLISION_CODE
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

with staging as (
{{ dbtvault.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=none) }}
)
              
select *
from staging
where PARENT_ID IS NOT NULL