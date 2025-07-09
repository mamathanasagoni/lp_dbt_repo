{%snapshot scd_raw_customer%}
    {{
        config(
            target_schema='snapshots',
            
            unique_key='Customer_ID',
            strategy='check',
            check_cols=['Customer_State'],  
            invalidate_hard_deletes=True
        )
    }}
 
    select * from AIRBNB.RAW.RAW_CUSTOMER
{% endsnapshot %}
 
 