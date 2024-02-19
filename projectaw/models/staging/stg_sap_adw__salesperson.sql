--talvez apagar a salesperson ou criar o Employee.BusinessEntityID
with 
    sales_person_data as (
        select 
            businessentityid as business_entity_id --ver se apaga HumanResources.Employee
            , territoryid as territory_id  --ver se apaga Sales.SalesTerritory
            , salesquota
            , bonus
            , commissionpct
            , salesytd
            , saleslastyear
            , rowguid
            --, modifieddate
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'salesperson') }}
    )

select *
from sales_person_data