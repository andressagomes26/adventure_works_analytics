with 
    sales_territory_data as (
        select 
            territoryid as territory_id
            , name as territory_name
            , countryregioncode as country_region_code
            , "group" as territory_group
            , rowguid
            , date(modifieddate) as modified_date --, modifieddate
        from {{ source('sap_adw', 'salesterritory') }}
    )

select *
from sales_territory_data