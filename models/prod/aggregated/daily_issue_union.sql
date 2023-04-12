{{ config(
  materialized='table'
) }}



{{ dbt_utils.union_relations(
    relations=[ref('daily_issue_form_clean'),ref('daily_issue_form_production')]
) }}