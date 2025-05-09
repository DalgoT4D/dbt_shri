------------------- Explanation of the Full outer join ----------------------

-- A FULL OUTER JOIN is a type of join operation in SQL that returns all rows from both tables being joined, 
-- along with matching rows from both tables where available. If there is no match, the result will contain 
-- null values for the columns of the table that does not have a matching row.

-- In the context of the query you provided, the FULL OUTER JOIN is used to combine the results of the my_cte
-- CTE and the data_passerbyuse_clean table. The join condition is based on the facility and date_auto columns.
-- The resulting table will contain all rows from both tables, with null values in the columns where there is 
-- no match.

-- For example, if there is a row in the my_cte CTE with a facility and date_auto value that does not exist in 
-- the data_passerbyuse_clean table, the resulting row will contain null values for the columns from the data_passerbyuse_clean table.

-- Similarly, if there is a row in the data_passerbyuse_clean table with a facility and date_auto value that does 
-- not exist in the my_cte CTE, the resulting row will contain null values for the columns from the my_cte CTE.


------------------- Explaination of the Query ----------------------

-- This SQL query is used to extract data from two tables, use_tracking and data_passerbyuse_clean, and join 
-- them together using a full outer join. The query selects several columns from the tables and applies some 
-- transformations to the data. 

-- The WITH clause defines a common table expression (CTE) called my_cte that selects columns from the use_tracking 
-- table and groups the data by date_auto and facility. The SUM function is used to count the number of men and women 
-- who used the facility on each day.

-- The main query selects columns from the my_cte CTE and the data_passerbyuse_clean table and applies some additional 
-- transformations to the data. The COALESCE function is used to handle null values. The resulting data is returned as a result set.

{{ config(
  materialized='table'
) }}

with my_cte as (
    select
        date_auto,
        facility, 
        sum(
            case
                when gender = 'male' then 1 
                else 0
            end
        ) as men_regular_number, 
        sum(
            case
                when gender = 'female' then 1 
                else 0
            end
        ) as women_regular_number

    from {{ ref('use_tracking') }}
    group by
        date_auto, facility
    order by date_auto desc
)

select 
    coalesce(a.facility, b.facility) as facility, 
    coalesce(a.date_auto, b.date_auto) as date_auto,
    coalesce(a.men_regular_number, 0) as men_regular_number,
    coalesce(a.women_regular_number, 0) as women_regular_number,
    coalesce(b.girl_number, 0) as girl_number, 
    coalesce(b.boy_number, 0) as boy_number,
    coalesce(b.woman_number, 0) as woman_number,
    coalesce(b.man_number, 0) as man_number,
    coalesce(a.women_regular_number, 0) + coalesce(b.woman_number, 0) as women_use,
    coalesce(b.girl_number, 0) as girls_use,
    coalesce(a.men_regular_number, 0) + coalesce(b.man_number, 0) as men_use,
    coalesce(b.boy_number, 0) as boys_use,
    coalesce(b.highlow_usage_level, '') as highlow_usage_level, 
    coalesce(b.woman_number, 0) + coalesce(b.girl_number, 0) + coalesce(b.man_number, 0) + coalesce(b.boy_number, 0) + coalesce(a.men_regular_number, 0) + coalesce(a.women_regular_number, 0) as total_use
from
    my_cte as a
full outer join
    {{ ref('data_passerbyuse_clean_reduced') }} as b
    on
        a.facility = b.facility
        and a.date_auto = b.date_auto    
 