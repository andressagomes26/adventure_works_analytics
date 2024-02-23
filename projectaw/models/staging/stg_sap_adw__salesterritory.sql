with 
    sales_territory_data as (
        select 
            territoryid as territory_id
            , case
                when name = 'Australia' then 'Austrália'
                when name = 'Canada' then 'Canadá'
                when name = 'Germany' then 'Alemanha'
                when name = 'France' then 'França'
                when name = 'United Kingdom' then 'Reino Unido'
                when name = 'Southeast' then 'Sudeste'
                when name = 'Northwest' then 'Noroeste'
                when name = 'Southwest' then 'Sudoeste'
                when name = 'Central' then 'Central'
                when name = 'Northeast' then 'Nordeste'
            end as territory_name
            , countryregioncode as country_region_code
            , "group" as territory_group
            , rowguid
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'salesterritory') }}
    )

select *
from sales_territory_data