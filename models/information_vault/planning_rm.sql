with planning_calendar as (
    select *
    from {{ ref('planning_calendar')}}
    where MONTH(date) = 7 OR MONTH(date) = 8
),
rm_users_hub as (
    select
        STAFF_HASH_KEY, 
        STAFF_BUSINESS_KEY,
        EMAIL_USERNAME_FK,
        BUSINESS_KEY_COLLISION_CODE
    from {{ ref('hub_staff')}}
    where BUSINESS_KEY_COLLISION_CODE = 'RESOURCE_MANAGER'
),
rm_assignments_link as (
    select
        ASSIGNMENT_HASH_KEY,
        LINE_ITEM_HASH_KEY,
        STAFF_HASH_KEY
    from {{ ref('link_staff_line_item')}}
),
rm_assignments_link_sat_max as (
    SELECT
        ASSIGNMENT_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_staff_line_item')}}
    GROUP BY ASSIGNMENT_HASH_KEY
),
rm_assignments_link_sat as (
    SELECT
        rm_a.ASSIGNMENT_HASH_KEY,
        rm_a.starts_at,
        rm_a.ends_at,
        rm_a.description,
        rm_a.hours_per_day,
        rm_a.LOAD_DATE_TIMESTAMP
    FROM {{ ref('sat_staff_line_item')}}  rm_a
    INNER JOIN rm_assignments_link_sat_max max_sat
    ON max_sat.MAX_TIMESTAMP = rm_a.LOAD_DATE_TIMESTAMP
    AND max_sat.ASSIGNMENT_HASH_KEY = rm_a.ASSIGNMENT_HASH_KEY
),
rm_assignments_link_detail as (
    select
        rm_al.ASSIGNMENT_HASH_KEY,
        rm_al.LINE_ITEM_HASH_KEY,
        rm_al.STAFF_HASH_KEY,
        rm_als.starts_at,
        rm_als.ends_at,
        rm_als.description,
        rm_als.hours_per_day,
        rm_als.LOAD_DATE_TIMESTAMP
    from rm_assignments_link rm_al
    join rm_assignments_link_sat rm_als
    ON rm_als.ASSIGNMENT_HASH_KEY=rm_al.ASSIGNMENT_HASH_KEY
),
rm_phases_hub as (
    SELECT
        LINE_ITEM_HASH_KEY,
        LINE_ITEM_BUSINESS_KEY,
        PROJECT_CODE_HASH_KEY, --Should be renamed to add FK to name?
        BUSINESS_KEY_COLLISION_CODE
    FROM {{ ref('hub_line_item')}}
    where BUSINESS_KEY_COLLISION_CODE = 'RESOURCE_MANAGER'    
),
rm_phases_hub_sat_max as (
    SELECT
        LINE_ITEM_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_line_item_rm')}}
    GROUP BY LINE_ITEM_HASH_KEY
),
rm_phases_hub_sat as (
    SELECT
        rm_p.LINE_ITEM_HASH_KEY,
        rm_p.project_code,
        rm_p.project_state,
        rm_p.name,
        rm_p.description,
        rm_p.starts_at,
        rm_p.ends_at,
        rm_p.LOAD_DATE_TIMESTAMP
    FROM {{ ref('sat_line_item_rm')}}  rm_p
    INNER JOIN rm_phases_hub_sat_max max_sat
    ON max_sat.MAX_TIMESTAMP = rm_p.LOAD_DATE_TIMESTAMP
    AND max_sat.LINE_ITEM_HASH_KEY = rm_p.LINE_ITEM_HASH_KEY
),
sal_staff as (
    select
        EMAIL_USERNAME_HASH_KEY,
        STAFF_HASH_KEY
    from {{ref('sal_staff')}}
    WHERE BUSINESS_KEY_COLLISION_CODE = 'RESOURCE_MANAGER'
),
active_rm_users as (
    select
        STAFF_HASH_KEY
    from {{ ref('active_rm_users_hub')}}
),
active_sal_staff as (
    select
        sal_staff.EMAIL_USERNAME_HASH_KEY,
        sal_staff.STAFF_HASH_KEY
    from sal_staff
    INNER JOIN active_rm_users ON sal_staff.STAFF_HASH_KEY = active_rm_users.STAFF_HASH_KEY
),
sal_line_item as (
    SELECT
        SAL_LINE_ITEM_HASH_KEY,
        LINE_ITEM_HASH_KEY
    from {{ref('sal_line_item')}}
    WHERE BUSINESS_KEY_COLLISION_CODE = 'RESOURCE_MANAGER'
)



SELECT 
	cal.email_address AS consultant_email,
	cal.full_name AS consultant,
	cal.date AS engagement_date,
	rm_ps.name AS planned,
	rm_ps.project_code,
	rm_ps.project_state AS engagement_status,
	SPLIT_PART(rm_ps.description, ' | ',1) AS hubspot_deal_id,
	SPLIT_PART(rm_ps.description, ' | ',2) AS hubspot_line_item_id,
    SPLIT_PART(rm_ps.description, ' | ',3) AS hubspot_line_item_name,
	COALESCE(rm_al.hours_per_day, 8) AS hours, -- Default to 8
	rm_al.description AS rm_assignment_description,
    rm_p.LINE_ITEM_HASH_KEY,
    sal_line_item.SAL_LINE_ITEM_HASH_KEY,
    sal.staff_hash_key as rm_staff_hash_key
FROM planning_calendar cal
    INNER JOIN active_sal_staff sal ON cal.EMAIL_USERNAME_HASH_KEY = sal.EMAIL_USERNAME_HASH_KEY
    inner JOIN rm_users_hub rm_u ON rm_u.STAFF_HASH_KEY = sal.STAFF_HASH_KEY
    left JOIN rm_assignments_link_detail rm_al
        ON cal.date >= rm_al.starts_at AND cal.date <= rm_al.ends_at
        AND rm_al.STAFF_HASH_KEY = rm_u.STAFF_HASH_KEY
    left JOIN rm_phases_hub rm_p ON rm_al.LINE_ITEM_HASH_KEY = rm_p.LINE_ITEM_HASH_KEY
    left JOIN rm_phases_hub_sat rm_ps ON rm_ps.LINE_ITEM_HASH_KEY = rm_p.LINE_ITEM_HASH_KEY
    left join sal_line_item on sal_line_item.LINE_ITEM_HASH_KEY = rm_p.LINE_ITEM_HASH_KEY




