with 
    country_region_data as (
        select 
            countryregioncode as country_region_code
            , case
                when name = 'France' then 'França'
                when name = 'Canada' then 'Canadá'
                when name = 'United States' then 'Estados Unidos'
                when name = 'Germany' then 'Alemanha'
                when name = 'United Kingdom' then 'Reino Unido'
                when name = 'Australia' then 'Austrália'
            end as country_region_name
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'countryregion') }}
    )

select *
from country_region_data