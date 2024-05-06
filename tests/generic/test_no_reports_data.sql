{% test no_reports_data(model) %}

SELECT *
FROM {{ model }}
WHERE date > CURRENT_DATE

{% endtest %}
