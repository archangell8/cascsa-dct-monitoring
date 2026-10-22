{% if table == 'F0005' %}
    {% set table_name = 'prodctl_F0005' %}
{% else %}
    {% set table_name = table %}
{% endif %}

SELECT
    '{{ table }}' AS TABELA,
    MAX(ROUND(KAFKA_CREATETIME/1000, 0)::BIGINT::TIMESTAMP) AS MAX_KAFKA_CREATETIME_{{ env }},
    MAX(KAFKA_OFFSET) AS MAX_KAFKA_OFFSET_{{ env }}
FROM
    {% if env == 'prd' %}
        class_prd.jde_class_raw_prd.{{ table_name }}
    {% else %}
        class_uat.jde_class_raw_uat.{{ table_name }}
    {% endif %}
WHERE
    1 = 1