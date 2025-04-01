{%- set input_relation = ref('daily_issue_form') -%}
{%- set issue_dict = get_issue_column_mapping('daily_issue_form') -%}

with exploded as (
  select
    _id,
    _submitted_by,
    facilityname as facility,
    minorissue_type,
    shift_type,
    date_auto,
    time_auto,
    _submission_time,
    
    -- Build an array of issue names (as string literals)
    unnest(array[
      {%- for key, mapping in issue_dict | dictsort %}
        '{{ key }}'{{ "," if not loop.last }}
      {%- endfor %}
    ]) as issue,
    
    -- Build an array of category values (as string literals)
    unnest(array[
      {%- for key, mapping in issue_dict | dictsort %}
        '{{ mapping.category }}'{{ "," if not loop.last }}
      {%- endfor %}
    ]) as category,
    
    -- For each metric, if the mapping exists output the column reference; otherwise output null.
    unnest(array[
      {%- for key, mapping in issue_dict | dictsort %}
        {{ mapping.outage if mapping.outage is defined else 'null' }}{{ "," if not loop.last }}
      {%- endfor %}
    ]) as outage,
    
    unnest(array[
      {%- for key, mapping in issue_dict | dictsort %}
        {{ mapping.fixed if mapping.fixed is defined else 'null' }}{{ "," if not loop.last }}
      {%- endfor %}
    ]) as fixed,
    
    unnest(array[
      {%- for key, mapping in issue_dict | dictsort %}
        {{ mapping.full if mapping.full is defined else 'null' }}{{ "," if not loop.last }}
      {%- endfor %}
    ]) as full_part,
    
    unnest(array[
      {%- for key, mapping in issue_dict | dictsort %}
        {{ mapping.hours if mapping.hours is defined else 'null' }}{{ "," if not loop.last }}
      {%- endfor %}
    ]) as num_hours,
    
    unnest(array[
      {%- for key, mapping in issue_dict | dictsort %}
        {{ mapping.fullfacility if mapping.fullfacility is defined else 'null' }}{{ "," if not loop.last }}
      {%- endfor %}
    ]) as full_facility,
    
    unnest(array[
      {%- for key, mapping in issue_dict | dictsort %}
        {{ mapping.stalls if mapping.stalls is defined else 'null' }}{{ "," if not loop.last }}
      {%- endfor %}
    ]) as stalls,
    
    unnest(array[
      {%- for key, mapping in issue_dict | dictsort %}
        {{ mapping.sides if mapping.sides is defined else 'null' }}{{ "," if not loop.last }}
      {%- endfor %}
    ]) as sides

  from {{ input_relation }}
)

select * from exploded