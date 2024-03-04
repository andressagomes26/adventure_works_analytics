with 
    stg_sales_order_header as (
        select 
            distinct(sales_order_id),
            sales_person_id,
            territory_id
        from {{ ref('stg_sap_adw__salesorderheader') }}
    )

    , stg_sales_person as (
        select 
            business_entity_id
        from {{ ref('stg_sap_adw__salesperson') }}
    )

    , stg_employee as (
        select 
            business_entity_id
            , loginid
            , jobtitle
            , gender
            , currentflag
        from {{ ref('stg_sap_adw__employee') }}
    )

    , stg_sales_territory as (
        select 
            territory_id
            , territory_name
            , country_region_code
        from {{ ref('stg_sap_adw__salesterritory') }}
    )

    , stg_country_region as (
        select 
            country_region_code
            , country_region_name
        from {{ ref('stg_sap_adw__countryregion') }}
    )

    , transformed_data as (
        select
            stg_sales_order_header.sales_person_id
            , stg_sales_order_header.sales_order_id
            , stg_employee.loginid
            , stg_employee.jobtitle
            , stg_employee.gender
            , stg_employee.currentflag
            , stg_country_region.country_region_name
        from stg_sales_order_header
        left join stg_sales_person
            on stg_sales_order_header.sales_person_id = stg_sales_person.business_entity_id
        left join stg_employee
            on stg_sales_person.business_entity_id = stg_employee.business_entity_id
        left join stg_sales_territory
            on stg_sales_order_header.territory_id = stg_sales_territory.territory_id
        left join stg_country_region
            on  stg_sales_territory.country_region_code = stg_country_region.country_region_code
        where stg_sales_order_header.sales_person_id is not null
    )

    , aggregated_data as (
        select
            {{ dbt_utils.generate_surrogate_key([
                'country_region_name'
                , 'jobtitle'
                , 'gender']) 
            }} as agg_sales_region_person_sk
            , country_region_name
            , jobtitle
            , gender
            , count(sales_order_id) as total_sales_orders
        from transformed_data
        group by
            country_region_name
            , jobtitle
            , gender
    )

select *
from aggregated_data
