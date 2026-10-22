SELECT
    '{{ table }}' AS TABELA,
    TO_DATE(CONVERT_TIMEZONE('America/Sao_Paulo', CURRENT_DATE)) AS AUDIT_DATE,
    COUNT(*) AS QTD_TARGET
FROM
    {% if env == 'prd' %}
        class_prd.pscfl_class_raw_prd.{{ table }}
    {% else %}
        class_qas.pscfl_class_raw_qas.{{ table }}
    {% endif %}
WHERE
    1 = 1
