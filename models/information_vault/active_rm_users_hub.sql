with rm_users_hub as (
    select
        STAFF_HASH_KEY, 
        STAFF_BUSINESS_KEY,
        EMAIL_USERNAME_FK,
        BUSINESS_KEY_COLLISION_CODE
    from {{ ref('hub_staff')}}
    where BUSINESS_KEY_COLLISION_CODE = 'RESOURCE_MANAGER'
),
rm_users_hub_sat_max as (
    SELECT
        STAFF_HASH_KEY,
        MAX(LOAD_DATE_TIMESTAMP) AS MAX_TIMESTAMP
    FROM {{ ref('sat_staff_rm')}}
    GROUP BY STAFF_HASH_KEY
),
rm_users_hub_sat as (
    SELECT
        rm_s.STAFF_HASH_KEY,
        rm_s.archived,
        rm_s.deleted
    FROM {{ ref('sat_staff_rm')}}  rm_s
    INNER JOIN rm_users_hub_sat_max max_sat
    ON max_sat.MAX_TIMESTAMP = rm_s.LOAD_DATE_TIMESTAMP
    AND max_sat.STAFF_HASH_KEY = rm_s.STAFF_HASH_KEY
)

select
    uh.STAFF_HASH_KEY,
    uh.STAFF_BUSINESS_KEY,
    uh.EMAIL_USERNAME_FK,
    uh.BUSINESS_KEY_COLLISION_CODE
from rm_users_hub uh
INNER JOIN rm_users_hub_sat uhs ON uhs.STAFF_HASH_KEY=uh.STAFF_HASH_KEY
WHERE uhs.archived = FALSE and uhs.deleted = FALSE