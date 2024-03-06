with
    negative_value as (
        select 
            totaldue
        from {{ source('sap_adw', 'salesorderheader') }}
    )

select *
from negative_value
where totaldue < 0