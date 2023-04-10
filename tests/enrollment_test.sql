version: 2

tests:
  - name: test_no_duplicate_values_in_column
  
    run:
      - expect(select count(*) from {{ ref('enrollment_production_dedup_remove') }}) = 
        select count(distinct 'dataid') from {{ ref('enrollment_production_dedup_remove') }}
