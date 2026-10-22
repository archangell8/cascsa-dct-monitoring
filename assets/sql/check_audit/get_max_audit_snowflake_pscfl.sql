SELECT
    '{{ table }}' AS TABELA,
    MAX({{ audit_column }}) AS MAX_AUDIT_DATE_SNOWFLAKE
FROM
    {% if env == 'prd' %}
        class_prd.pscfl_class_raw_prd.{{ table }}
    {% else %}
        class_qas.pscfl_class_raw_qas.{{ table }}
    {% endif %}
WHERE
    1 = 1