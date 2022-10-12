with planning_rm as (
    select *
    from {{ ref('planning_rm')}}
),
sal_line_item as (
    select
        SAL_LINE_ITEM_HASH_KEY,
        LINE_ITEM_HASH_KEY
    from {{ref('sal_line_item')}}
    WHERE BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
),
hub_line_item as (
    select *
    from {{ ref('hub_line_item')}}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
),
hubspot_line_item_hub_sat_max as (
    SELECT
        LINE_ITEM_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_line_item_hubspot')}}
    GROUP BY LINE_ITEM_HASH_KEY
),
hubspot_line_item_hub_sat as (
    SELECT
        hli.LINE_ITEM_HASH_KEY,
        hli.property_price,
        hli.property_quantity,
        hli.LOAD_DATE_TIMESTAMP
    FROM {{ ref('sat_line_item_hubspot')}}  hli
    INNER JOIN hubspot_line_item_hub_sat_max max_sat
    ON max_sat.MAX_TIMESTAMP = hli.LOAD_DATE_TIMESTAMP
    AND max_sat.LINE_ITEM_HASH_KEY = hli.LINE_ITEM_HASH_KEY
),
link_line_item_deal as (
    select *
    from {{ ref('link_line_item_deal')}}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
),
hub_deal as (
    select *
    from {{ ref('hub_deal')}}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
),
link_deal_company as (
    select *
    from {{ ref('link_deal_company')}}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
),
hub_company as (
    select *
    from {{ ref('hub_company')}}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
),
hubspot_company_hub_sat_max as (
    SELECT
        CLIENT_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_company')}}
    GROUP BY CLIENT_HASH_KEY
),
hubspot_company_hub_sat as (
    SELECT
        hc.CLIENT_HASH_KEY,
        hc.property_name,
        hc.property_biztory_shortcode,
        hc.property_address,
        hc.property_city,
        hc.property_zip,
        hc.property_state,
        hc.property_country
    FROM {{ ref('sat_company')}}  hc
    INNER JOIN hubspot_company_hub_sat_max max_sat
    ON max_sat.MAX_TIMESTAMP = hc.LOAD_DATE_TIMESTAMP
    AND max_sat.CLIENT_HASH_KEY = hc.CLIENT_HASH_KEY
),
link_deal_staff as (
    select *
    from {{ ref('link_deal_staff')}}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
),
hub_staff_hubspot as (
    select *
    from {{ ref('hub_staff')}}
    where BUSINESS_KEY_COLLISION_CODE = 'HUBSPOT'
),
hubspot_staff_hub_sat_max as (
    SELECT
        STAFF_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_staff_hubspot')}}
    GROUP BY STAFF_HASH_KEY
),
hubspot_staff_hub_sat as (
    SELECT
        hs.STAFF_HASH_KEY,
        hs.email
    FROM {{ ref('sat_staff_hubspot')}}  hs
    INNER JOIN hubspot_staff_hub_sat_max max_sat
    ON max_sat.MAX_TIMESTAMP = hs.LOAD_DATE_TIMESTAMP
    AND max_sat.STAFF_HASH_KEY = hs.STAFF_HASH_KEY
),
sal_staff_rm as (
    select *
    from {{ ref('sal_staff')}}
    where BUSINESS_KEY_COLLISION_CODE = 'RESOURCE_MANAGER'
),
sal_staff_biztory_team as (
    select *
    from {{ ref('sal_staff')}}
    where BUSINESS_KEY_COLLISION_CODE = 'GSHEETS'
),
hub_staff_bt as (
    select *
    from {{ ref('hub_staff')}}
    where BUSINESS_KEY_COLLISION_CODE = 'GSHEETS'
),
bt_staff_hub_sat_max as (
    SELECT
        STAFF_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_staff_biztory_team')}}
    GROUP BY STAFF_HASH_KEY
),
bt_staff_hub_sat as (
    SELECT
        bts.STAFF_HASH_KEY,
        bts.full_name,
        bts.reports_to,
        bts.start_date,
        bts.end_date,
        bts.cronos_login,
        bts.team,
        bts.biztory_branch,
        bts.status,
        bts.member_type,
        bts.role,
        bts.country
    FROM {{ ref('sat_staff_biztory_team')}}  bts
    INNER JOIN bt_staff_hub_sat_max max_sat
    ON max_sat.MAX_TIMESTAMP = bts.LOAD_DATE_TIMESTAMP
    AND max_sat.STAFF_HASH_KEY = bts.STAFF_HASH_KEY
)


SELECT
    bp.consultant_email,
    hsbts.full_name AS consultant, 
    bp.engagement_date,
    bp.planned,
    bp.engagement_status,
    bp.hours,
    bp.rm_assignment_description,
    bp.project_code,
    hlis.property_price AS bill_rate,
    hlis.property_quantity AS line_item_num_days,
    hd.deal_business_key AS planned_deal_id,
    hc.client_business_key AS planned_company_id,
    hcs.property_name AS planned_company_name,
    LOWER(hcs.property_biztory_shortcode) AS planned_biztory_shortcode,
    hcs.property_address AS planned_company_address,
    hcs.property_city AS planned_company_city,
    hcs.property_zip AS planned_company_zip,
    hcs.property_state AS planned_company_state,
    hcs.property_country AS planned_company_country,
    hshs.email AS planned_deal_owner,
    -- From Team
    hsbts.reports_to AS consultant_reports_to,
    hsbts.start_date AS consultant_start_date,
    hsbts.end_date AS consultant_end_date,
    hsbts.cronos_login AS consultant_cronos_login,
    hsbts.team AS consultant_team,
    hsbts.biztory_branch AS consultant_biztory_branch,
    hsbts.status AS consultant_status,
    hsbts.member_type AS consultant_member_type,
    hsbts.role AS consultant_role,
    hsbts.country AS consultant_country
FROM planning_rm bp
    LEFT JOIN sal_line_item sli ON sli.SAL_LINE_ITEM_HASH_KEY = bp.SAL_LINE_ITEM_HASH_KEY
    LEFT JOIN hub_line_item hli ON hli.LINE_ITEM_HASH_KEY = sli.LINE_ITEM_HASH_KEY
    LEFT JOIN hubspot_line_item_hub_sat hlis ON hlis.LINE_ITEM_HASH_KEY = hli.LINE_ITEM_HASH_KEY
    LEFT JOIN link_line_item_deal llid ON llid.LINE_ITEM_HASH_KEY = hli.LINE_ITEM_HASH_KEY
    LEFT JOIN hub_deal hd ON hd.DEAL_HASH_KEY = llid.DEAL_HASH_KEY
    LEFT JOIN link_deal_company ldc ON ldc.DEAL_HASH_KEY = hd.DEAL_HASH_KEY
    LEFT JOIN hub_company hc ON hc.CLIENT_HASH_KEY = ldc.CLIENT_HASH_KEY
    LEFT JOIN hubspot_company_hub_sat hcs ON hcs.CLIENT_HASH_KEY = hc.CLIENT_HASH_KEY
    LEFT JOIN link_deal_staff lds ON lds.DEAL_HASH_KEY = hd.DEAL_HASH_KEY
    LEFT JOIN hub_staff_hubspot hsh ON hsh.STAFF_HASH_KEY = lds.STAFF_HASH_KEY
    LEFT JOIN hubspot_staff_hub_sat hshs ON hshs.STAFF_HASH_KEY = hsh.STAFF_HASH_KEY
    LEFT JOIN sal_staff_rm ssrm ON ssrm.STAFF_HASH_KEY = bp.RM_STAFF_HASH_KEY
    LEFT JOIN sal_staff_biztory_team ssbt ON ssbt.EMAIL_USERNAME_HASH_KEY = ssrm.EMAIL_USERNAME_HASH_KEY
    LEFT JOIN hub_staff_bt hsbt ON hsbt.STAFF_HASH_KEY = ssbt.STAFF_HASH_KEY
    LEFT JOIN bt_staff_hub_sat hsbts ON hsbts.STAFF_HASH_KEY = hsbt.STAFF_HASH_KEY
    
