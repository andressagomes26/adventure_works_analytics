with 
    test_dim_locations as (
        select 
            location_sk
            , city
            , state_province_name
            , country_region_code
            , country_region_name
        from {{ ref('dim_locations') }}
        where
            location_sk = '225325a0029a6046cc81731ac8bec58f'
    )

    , test_validation as (
        select 
            * 
        from test_dim_locations 
        where 
            city != 'Saint Ouen'
            or country_region_code != 'FR'
            or country_region_name != 'França'
    )

select * 
from test_validation