WITH scd_raw_customer AS (
    SELECT * FROM {{ref('scd_raw_customer')}}
)
select * from scd_raw_customer
where DBT_VALID_TO is null