with
    dates_raw as (
        {{
            dbt_utils.date_spine(
                datepart="day"
                , start_date= "cast('2011-01-01' as date)" 
                , end_date="cast('2015-01-01' as date)"
            )
        }}
    )

    , date_columns as (
        select distinct 
            cast(date_day as date) as metric_date
            , extract(day from date_day) as metric_day
            , extract(month from date_day) as metric_month
            , extract(year from date_day) as metric_year
            , extract(quarter from date_day) as metric_quarter
            , case
                when extract(quarter from date_day) = 1 then 1
                when extract(quarter from date_day) = 2 then 1
                when extract(quarter from date_day) = 3 then 2
                when extract(quarter from date_day) = 4 then 2
            end as semester   
            , case
                when extract(dayofweek from date_day) = 1 then 'Domingo'
                when extract(dayofweek from date_day) = 2 then 'Segunda'
                when extract(dayofweek from date_day) = 3 then 'Terça'
                when extract(dayofweek from date_day) = 4 then 'Quarta'
                when extract(dayofweek from date_day) = 5 then 'Quinta'
                when extract(dayofweek from date_day) = 6 then 'Sexta'
                when extract(dayofweek from date_day) = 7 then 'Sábado'
            end as dayofweek
            , case
                when extract(month from date_day) = 1 then 'Janeiro'
                when extract(month from date_day) = 2 then 'Fevereiro'
                when extract(month from date_day) = 3 then 'Março'
                when extract(month from date_day) = 4 then 'Abril'
                when extract(month from date_day) = 5 then 'Maio'
                when extract(month from date_day) = 6 then 'Junho'
                when extract(month from date_day) = 7 then 'Julho'
                when extract(month from date_day) = 8 then 'Agosto'
                when extract(month from date_day) = 9 then 'Setembro'
                when extract(month from date_day) = 10 then 'Outubro'
                when extract(month from date_day) = 11 then 'Novembro'
                when extract(month from date_day) = 12 then 'Dezembro'
            end as fullmonth
        from dates_raw
    )

select *
from date_columns