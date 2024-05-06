{% test uses_no_report(model) %}

SELECT *
FROM {{ model }}
WHERE date_auto > CURRENT_DATE

{% endtest %}
