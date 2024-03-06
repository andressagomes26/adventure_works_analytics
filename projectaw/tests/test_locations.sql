with 
    test_state_province as (
        select 
            stateprovinceid
            , stateprovincecode
            , countryregioncode
            , name
        from {{ source('sap_adw', 'stateprovince') }}
        where
            stateprovinceid = 2
    )

    , test_validation as (
        select 
            * 
        from test_state_province 
        where 
            stateprovincecode != 'AK '
            or countryregioncode != 'US'
            or name != 'Alaska'
    )

select * 
from test_validation