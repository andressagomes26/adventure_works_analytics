with 
    sales_person_data as (
        select 
            businessentityid
            , territoryid
            , salesquota
            , bonus
            , commissionpct
            , salesytd
            , saleslastyear
            , rowguid
            , modifieddate
        from {{ source('sap_adw', 'salesperson') }}
    )

select *
from sales_person_data