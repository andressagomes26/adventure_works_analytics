with
    state_province_data as (
        select 
            stateprovinceid as state_province_id
            , stateprovincecode as state_province_code
            , countryregioncode as country_region_code
            , isonlystateprovinceflag
            , name as state_province_name
            , territoryid as territory_id
            , rowguid
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'stateprovince') }}
    )

select *
from state_province_data
