{{
    config(
        materialized='incremental',
        unique_keys='id',
        incremental_strategy='merge'
    )
}}
SELECT
    id,
    
    name,
    updated_at
FROM airbnb.dev.customerdetails
 
{% if is_incremental() %}
 
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
 
{% endif %}

