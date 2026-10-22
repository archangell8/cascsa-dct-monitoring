{% if table == 'F0005' %}
    {% set table_name = 'prodctl_F0005' %}
{% else %}
    {% set table_name = table %}
{% endif %}

SELECT
    '{{ table }}' AS TABELA,
    COUNT(*) AS QTD_TARGET,
    TO_DATE(CONVERT_TIMEZONE('America/Sao_Paulo', CURRENT_DATE)) AS AUDIT_DATE
FROM
    {% if env == 'prd' %}
        class_prd.jde_class_raw_prd.{{ table_name }}
    {% else %}
        class_uat.jde_class_raw_uat.{{ table_name }}
    {% endif %}
WHERE
    1 = 1
    {% if table_name not in ('F41112', 'F5549401', 'F554349F', 'F5543081', 'F5576523', 'F554201B', 'F5576201') %}
    AND (COALESCE(OP_TYPE, '-') NOT IN ('0', '3') OR (OP_TYPE = '0' AND OP_TIME = 0))
    {% endif %}
