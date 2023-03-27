{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}


{{
    flatten_json(
        model_name = source('source_shri_surveys', 'raw_enrollment'),
        json_column = '_airbyte_data',
        json_fields_to_retain = ['_id', '_status', '_submitted_by', '_submission_time', 'begin_group_yeQ4kl9Kt/yob', 'begin_group_G8GvBDlis/dataid', 'begin_group_yeQ4kl9Kt/gender', 'begin_group_yeQ4kl9Kt/id_num', 'begin_group_yeQ4kl9Kt/name_c', 'begin_group_yeQ4kl9Kt/facility', 'begin_group_yeQ4kl9Kt/lastname', 'begin_group_yeQ4kl9Kt/firstname', 'begin_group_G8GvBDlis/lastname_c', 'begin_group_yeQ4kl9Kt/mothername', 'begin_group_G8GvBDlis/firstname_c', 'begin_group_G8GvBDlis/form-timestamp', 'begin_group_yeQ4kl9Kt/identification_c', 'begin_group_G8GvBDlis/form-timestamp-formatted'],
        non_json_column_fields = ['_airbyte_ab_id', '_airbyte_emitted_at']
    )
}}