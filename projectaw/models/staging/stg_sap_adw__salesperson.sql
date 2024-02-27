--talvez apagar a salesperson ou criar o Employee.BusinessEntityID
with 
    sales_person_data as (
        select 
            businessentityid as business_entity_id
            , territoryid as territory_id
            , salesquota
            , bonus
            , commissionpct
            , salesytd
            , saleslastyear
            , rowguid
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'salesperson') }}
    )

select *
from sales_person_data