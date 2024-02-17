with 
    test_dates as (
        select 
            metric_date 
            , metric_day
            , metric_month
            , metric_year
            , semester
            , dayofweek
            , fullmonth
        from {{ ref('dim_dates') }}
        where
            metric_date = '2011-05-31'
    )

    , test_validation as (
        select 
            * 
        from test_dates 
        where 
            metric_day != 31
            or metric_month != 5
            or metric_year != 2011
            or semester != 1
            or dayofweek != 'Terça'
            or fullmonth != 'Maio'
    )

select * 
from test_validation