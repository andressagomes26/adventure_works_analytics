with 
    stg_order_header as (
        select 
            distinct customer_id
        from {{ ref('stg_sap_adw__salesorderheader') }}
    )

    , stg_customer as (
        select 
            customer_id
            , person_id
        from {{ ref('stg_sap_adw__customer') }}
    )

    , stg_person as (
        select 
            business_entity_id
            , firstname
            , lastname
            , concat(firstname, ' ', lastname) AS fullname
            , case
                when persontype  = 'SC' then 'Contato da loja' 
                when persontype  = 'IN' then 'Cliente individual' 
                when persontype  = 'SP' then 'Vendedor'
                when persontype  = 'EM' then 'Funcionário' 
                when persontype  = 'VC' then 'Contato do fornecedor'
                when persontype  = 'GC' then 'Contato geral'
                else persontype
            end as person_type
        from {{ ref('stg_sap_adw__person') }}
    )

    , stg_store as (
        select 
            business_entity_id
            , name_store
        from {{ ref('stg_sap_adw__store') }}
    )

    , transformed_data as (
        select
            {{ dbt_utils.generate_surrogate_key(['stg_order_header.customer_id']) }} as location_sk
            , stg_order_header.customer_id
            , stg_person.business_entity_id
            , stg_person.firstname
            , stg_person.lastname
            , stg_person.fullname
            , stg_person.person_type
            , stg_store.name_store
        from stg_order_header 
        left join stg_customer 
            on stg_order_header.customer_id = stg_customer.customer_id
        left join stg_person
            on stg_customer.person_id = stg_person.business_entity_id
        left join stg_store
            on stg_person.business_entity_id = stg_store.business_entity_id
        
    )

select *
from transformed_data