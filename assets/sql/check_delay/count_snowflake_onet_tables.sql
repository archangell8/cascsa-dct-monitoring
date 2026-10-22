SELECT
    '{{ table }}' AS TABELA,
    COUNT(*) AS QTD_TARGET,
    TO_DATE(CONVERT_TIMEZONE('America/Sao_Paulo', CURRENT_DATE)) AS AUDIT_DATE
FROM
    {% if env == 'prd' %}
        class_prd.casc_sa_sm_raw_prd.{{ table }}
    {% else %}
        class_prd.casc_sa_sm_raw_qas.{{ table }}
    {% endif %}
WHERE
    1 = 1
