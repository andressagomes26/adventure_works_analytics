with 
    employee_data as (
        select 
            businessentityid as business_entity_id
            -- nationalidnumber
            , loginid
            , jobtitle
            -- birthdate
            -- maritalstatus
            , gender
            -- hiredate
            -- salariedflag
            -- vacationhours
            -- sickleavehours
            , currentflag
            -- rowguid
            -- modifieddate
            -- organizationnode
            --, modifieddate
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'employee') }}
    )

select *
from employee_data