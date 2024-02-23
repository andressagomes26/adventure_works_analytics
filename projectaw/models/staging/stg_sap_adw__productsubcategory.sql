with 
    product_subcategory_data as (
        select 
            productsubcategoryid as product_subcategory_id
            , productcategoryid as product_category_id
            , case
                when name = 'Mountain Bikes' then 'Bicicletas de montanha'
                when name = 'Road Bikes' then 'Bicicletas de estrada'
                when name = 'Touring Bikes' then 'Bicicletas de turismo'
                when name = 'Handlebars' then 'Guidão'
                when name = 'Bottom Brackets' then 'Colchetes Inferiores'
                when name = 'Brakes' then 'Freios'
                when name = 'Chains' then 'Correntes'
                when name = 'Cranksets' then 'Pedaleiras'
                when name = 'Derailleurs' then 'Desviadores'
                when name = 'Forks' then 'Garfos'
                when name = 'Headsets' then 'Fones de ouvido'
                when name = 'Mountain Frames' then 'Quadros de montanha'
                when name = 'Pedals' then 'Pedais'
                when name = 'Road Frames' then 'Quadros de estrada'
                when name = 'Saddles' then 'Selas'
                when name = 'Touring Frames' then 'Quadros de turismo'
                when name = 'Wheels' then 'Rodas'
                when name = 'Bib-Shorts' then 'Shorts'
                when name = 'Caps' then 'Cápsulas'
                when name = 'Gloves' then 'Luvas'
                when name = 'Jerseys' then 'Camisas'
                when name = 'Shorts' then 'Shorts'
                when name = 'Socks' then 'Meias'
                when name = 'Tights' then 'Meia-calça'
                when name = 'Vests' then 'Coletes'
                when name = 'Bike Racks' then 'Bicicletários'
                when name = 'Bike Stands' then 'Suportes para bicicletas'
                when name = 'Bottles and Cages' then 'Garrafas e gaiolas'
                when name = 'Cleaners' then 'Limpadores'
                when name = 'Fenders' then 'Pára-lamas'
                when name = 'Helmets' then 'Capacetes'
                when name = 'Hydration Packs' then 'Pacotes de hidratação'
                when name = 'Lights' then 'Luzes'
                when name = 'Locks' then 'Fechaduras'
                when name = 'Panniers' then 'Cestos'
                when name = 'Pumps' then 'Bombas'
                when name = 'Tires and Tubes' then 'Pneus e Câmaras de Ar'
            end as product_subcategory_name
            , rowguid
            , date(modifieddate) as modified_date
        from {{ source('sap_adw', 'productsubcategory') }}
    )

select *
from product_subcategory_data