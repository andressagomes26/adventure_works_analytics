with 
    dados_categorias as (
        select 
            *
        from {{ source('sap_adw', 'creditcard') }}
    )

select *
from dados_categorias