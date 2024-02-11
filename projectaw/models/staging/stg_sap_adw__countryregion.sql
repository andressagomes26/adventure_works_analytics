with 
    country_region_data as (
        select 
            countryregioncode
            , name
            , modifieddate
        from {{ source('sap_adw', 'countryregion') }}
    )

select *
from country_region_data