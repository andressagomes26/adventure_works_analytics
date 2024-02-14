select
  *
  -- stg_sap_adw__salesorderheader.sales_order_id
  -- , stg_sap_adw__salesorderheader.sales_person_id
  -- , employee.loginid
  -- , employee.jobtitle
  -- , employee.birthdate
  -- , employee.gender
  -- , stg_sap_adw__salesorderheader.territory_id
  -- , stg_sap_adw__salesterritory.territory_name
  -- , stg_sap_adw__salesterritory.country_region_code
from dev_andressa_staging.stg_sap_adw__salesorderheader
left join dev_andressa_staging.stg_sap_adw__salesperson
  on stg_sap_adw__salesorderheader.sales_person_id = stg_sap_adw__salesperson.business_entity_id
left join dev_andressa_sap_adw.employee
  on employee.businessentityid = stg_sap_adw__salesperson.business_entity_id
left join dev_andressa_staging.stg_sap_adw__salesterritory
  on stg_sap_adw__salesorderheader.territory_id = stg_sap_adw__salesterritory.territory_id
left join dev_andressa_staging.stg_sap_adw__countryregion
  on stg_sap_adw__countryregion.country_region_code = stg_sap_adw__salesterritory.country_region_code
where stg_sap_adw__salesorderheader.sales_person_id is not null


-- with 
--     stg_sales_order_header as (
--         select 
--             distinct(sales_order_id),
--             sales_person_id
--         from {{ ref('stg_sap_adw__salesorderheader') }}
--         where sales_person_id is not null
--     )
    
--     , stg_sales_person as (
--         select 
--             business_entity_id,
--             territory_id
--         from {{ ref('stg_sap_adw__salesperson') }}
--         where territory_id is not null
--     )
    

--     , stg_sales_territory as (
--         select 
--             territory_id
--             , territory_name
--             , country_region_code 
--             , territory_group
--         from {{ ref('stg_sap_adw__salesterritory') }}
--     )

--     , transformed_data as (
--         select
--             {{ dbt_utils.generate_surrogate_key(['stg_sales_order_header.sales_order_id']) }} as agg_sales_resgion_person_sk
--             , stg_sales_order_header.sales_order_id
--             , stg_sales_order_header.sales_person_id
--             , stg_sales_territory.territory_id
--             , stg_sales_territory.territory_name
--             , stg_sales_territory.country_region_code
--             , stg_sales_territory.territory_group
--         from stg_sales_order_header 
--         left join stg_sales_person 
--             on stg_sales_order_header.sales_person_id = stg_sales_person.business_entity_id
--         left join stg_sales_territory
--             on stg_sales_person.territory_id = stg_sales_territory.territory_id
--     )

-- select *
-- from transformed_data



-- with 
--     stg_sales_order_header as (
--       select 
--         distinct(sales_order_id)
--         , sales_person_id
--       from dev_andressa_staging.stg_sap_adw__salesorderheader
--       where sales_person_id is not null
--     )
    
--     , stg_sales_person as (
--       select 
--         business_entity_id
--         , territory_id
--       from dev_andressa_staging.stg_sap_adw__salesperson
--       where territory_id is not null
--     )

--     , stg_person as (
--         select 
--             business_entity_id
--             , firstname
--             , lastname
--             , concat(firstname, ' ', lastname) AS fullname
--         from dev_andressa_staging.stg_sap_adw__person
--         where persontype = 'SP' and business_entity_id is not null
--      )
--     , stg_sales_territory as (
--       select 
--           territory_id
--           , territory_name
--           , country_region_code 
--           , territory_group
--       from dev_andressa_staging.stg_sap_adw__salesterritory
--     )
--     , transformed_data as (
--         select
--             row_number() over (order by stg_sales_order_header.sales_order_id) as  agg_sales_resgion_person_sk
--             --, stg_sales_order_header.sales_order_id
--             --, stg_sales_order_header.sales_person_id
--             , stg_person.fullname
--             , stg_sales_territory.territory_id
--             , stg_sales_territory.territory_name
--             , stg_sales_territory.country_region_code
--             , stg_sales_territory.territory_group
--         from stg_sales_order_header 
--         left join stg_sales_person 
--             on stg_sales_order_header.sales_person_id = stg_sales_person.business_entity_id
--         left join stg_person 
--             on stg_sales_person.business_entity_id = stg_person.business_entity_id
--         left join stg_sales_territory
--             on stg_sales_person.territory_id = stg_sales_territory.territory_id
--     )

-- select *
-- from transformed_data