with 
    person_data as (
        select 
            businessentityid as business_entity_id
            , case
                when persontype  = 'SC' then 'Contato da loja' 
                when persontype  = 'IN' then 'Cliente individual' 
                when persontype  = 'SP' then 'Vendedor'
                when persontype  = 'EM' then 'Funcionário' 
                when persontype  = 'VC' then 'Contato do fornecedor'
                when persontype  = 'GC' then 'Contato geral'
                else persontype
            end as person_type
            , namestyle
            , title
            , firstname
            , middlename
            , lastname
            , suffix
            , emailpromotion
            , additionalcontactinfo
            , demographics
            , rowguid
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'person') }}
    )

select *
from person_data