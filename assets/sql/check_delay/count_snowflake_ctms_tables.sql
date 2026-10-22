SELECT
    '{{ table }}' AS TABELA,
    COUNT(*) AS QTD_TARGET,
    TO_DATE(CONVERT_TIMEZONE('America/Sao_Paulo', CURRENT_DATE)) AS AUDIT_DATE
FROM
    {% if env == 'prd' %}
        class_prd.ctms_class_raw_prd.{{ table }}
    {% else %}
        class_qas.ctms_class_raw_qas.{{ table }}
    {% endif %}
WHERE
    1 = 1
    AND CTMS_TIMESTAMP >= TO_DATE(CONVERT_TIMEZONE('America/Sao_Paulo', CURRENT_DATE)) - 7