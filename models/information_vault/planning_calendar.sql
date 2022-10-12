with calendar as (
    select
        date, 
        day_of_week, 
        week_of_year
    from {{ ref('calendar') }}
),
hub_staff_bt as (
    select
        STAFF_HASH_KEY, 
        STAFF_BUSINESS_KEY, 
        BUSINESS_KEY_COLLISION_CODE
    from {{ ref('hub_staff') }}
    where BUSINESS_KEY_COLLISION_CODE = 'GSHEETS'
),
max_sat_staff_biztory_team as (
    SELECT
        STAFF_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_staff_biztory_team') }}
    GROUP BY STAFF_HASH_KEY    
),
sat_staff_biztory_team as (
    select
        bt.STAFF_HASH_KEY,
        bt.EMAIL_ADDRESS,
        bt.FULL_NAME,
        bt.team,
        bt.country,
        bt.start_date,
        bt.end_date
    from {{ ref('sat_staff_biztory_team') }} bt
    INNER JOIN max_sat_staff_biztory_team max_bt_sat
    ON max_bt_sat.MAX_TIMESTAMP = bt.LOAD_DATE_TIMESTAMP
    AND max_bt_sat.STAFF_HASH_KEY = bt.STAFF_HASH_KEY
    where bt.role='Consultant' and status='Active' 
),
sal_staff as (
    select
        EMAIL_USERNAME_HASH_KEY,
        STAFF_HASH_KEY
    from {{ref('sal_staff')}}
    WHERE BUSINESS_KEY_COLLISION_CODE = 'GSHEETS'
)

SELECT
    sal_staff.EMAIL_USERNAME_HASH_KEY,
    sat_staff.email_address,
    sat_staff.FULL_NAME,
    sat_staff.team,
    sat_staff.country,
    cal.date::DATE as date,
    cal.week_of_year
FROM hub_staff_bt hub_staff
LEFT JOIN sat_staff_biztory_team sat_staff ON hub_staff.STAFF_HASH_KEY = sat_staff.STAFF_HASH_KEY
LEFT JOIN sal_staff ON sal_staff.STAFF_HASH_KEY = hub_staff.STAFF_HASH_KEY
    JOIN calendar cal
      ON cal.date >= sat_staff.start_date
      AND cal.date <= COALESCE(sat_staff.end_date, DATEADD(day, 365, CURRENT_DATE))
WHERE cal.day_of_week > 0 AND cal.day_of_week < 6