{{ config(
  materialized='table'
) }}


select
    date_auto,
    facility, 
    unnest(array['men','women','girls','boys']) as user,
    unnest(array[men_use,women_use,girls_use,boys_use]) as use

from {{ ref('usetracking_dashboard') }}
