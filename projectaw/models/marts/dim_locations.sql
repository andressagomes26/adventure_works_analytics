with 
    stg_order_header as (
        select 
            distinct shiptoaddressid
        from {{ ref('stg_sap_adw__salesorderheader') }}
    )

    , stg_address as (
        select 
            address_id
            , city
            , state_province_id
            , postalcode
        from {{ ref('stg_sap_adw__address') }}
    )

    , stg_state_province as (
        select 
            state_province_id
            , state_province_code
            , country_region_code
            , state_province_name
        from {{ ref('stg_sap_adw__stateprovince') }}
    )

    , stg_country_region as (
        select 
            country_region_code
            , country_region_name
        from {{ ref('stg_sap_adw__countryregion') }}
    )

    , transformed_data as (
        select
            {{ dbt_utils.generate_surrogate_key(['stg_order_header.shiptoaddressid']) }} as location_sk
            , stg_order_header.shiptoaddressid as address_id
            , stg_address.city
            , stg_address.postalcode
            , stg_state_province.state_province_code
            , stg_state_province.state_province_name
            , stg_country_region.country_region_code
            , stg_country_region.country_region_name
            -- , stg_address.address_id
            --, stg_address.state_province_id
            --, stg_state_province.state_province_id
            --, stg_state_province.country_region_cod
        from stg_order_header 
        left join stg_address 
            on stg_order_header.shiptoaddressid = stg_address.address_id
        left join stg_state_province
            on stg_address.state_province_id = stg_state_province.state_province_id
        left join stg_country_region
            on stg_state_province.country_region_code = stg_country_region.country_region_code
    )

select *
from transformed_data