with 
    person_data as (
        select 
            businessentityid
            , persontype
            , namestyle
            --, title
            , firstname
            , middlename
            , lastname
            --, suffix
            --, emailpromotion
            --, additionalcontactinfo
            --, demographics
            --, rowguid
            , date(modifieddate) as modified_date -- modifieddate
        from {{ source('sap_adw', 'person') }}
    )

select *
from person_data