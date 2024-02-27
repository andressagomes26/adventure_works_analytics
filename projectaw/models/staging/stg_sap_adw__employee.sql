with 
    employee_data as (
        select 
            businessentityid as business_entity_id
            , nationalidnumber
            , loginid
            , case
                when jobtitle = 'North American Sales Manager' then 'Gerente de Vendas Norte-Americano'
                when jobtitle = 'Sales Representative' then 'Representante de vendas'
                when jobtitle = 'European Sales Manager' then 'Gerente de Vendas Europeu'
                when jobtitle = 'Pacific Sales Manager' then 'Gerente de Vendas do Pacífico'
            end as jobtitle
            , birthdate
            , maritalstatus
            , gender
            , hiredate
            , salariedflag
            , vacationhours
            , sickleavehours
            , currentflag
            , rowguid
            , modifieddate
            , organizationnode
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'employee') }}
    )

select *
from employee_data