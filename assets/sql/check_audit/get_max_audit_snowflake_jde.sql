{% if table == 'F0005' %}
    {% set table_name = 'prodctl_F0005' %}
{% else %}
    {% set table_name = table %}
{% endif %}

SELECT
    '{{ table }}' AS TABELA,
    MAX(
        DATEADD(
            DAY,
            SUBSTR(({{ audit_column }}::INT + 1900000)::STRING, 5, 3)::INT - 1,
            TO_TIMESTAMP(
                CONCAT(SUBSTR(({{ audit_column }}::INT + 1900000)::STRING, 1, 4), '01', '01'),
                'YYYYMMDD'
            )
        )
    ) AS MAX_AUDIT_DATE_SNOWFLAKE
FROM
    {% if env == 'prd' %}
        class_prd.jde_class_raw_prd.{{ table_name }}
    {% else %}
        class_uat.jde_class_raw_uat.{{ table_name }}
    {% endif %}
WHERE
    1 = 1
