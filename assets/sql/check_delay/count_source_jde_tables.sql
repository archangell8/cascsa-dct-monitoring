{% set n_days = range(0, num_days) %}

{% for n_day in n_days %}
SELECT
    '{{ table }}' AS TABELA,
    COUNT(*) AS QTD_SOURCE,
    '{{ current_julian_date - n_day }}' AS JULIAN_AUDIT_DATE
FROM {{ schema }}.{{ table }}
WHERE
    1 = 1
    AND {{ audit_column }} = {{ current_julian_date - n_day }}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
