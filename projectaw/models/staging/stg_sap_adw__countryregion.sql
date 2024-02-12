with 
    country_region_data as (
        select 
            countryregioncode as country_region_code
            , name as country_region_name
            , date(modifieddate) as modified_date
            --, modifieddate 
        from {{ source('sap_adw', 'countryregion') }}
    )

select *
from country_region_data