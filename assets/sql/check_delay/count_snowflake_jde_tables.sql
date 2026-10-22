{% if table == 'F0005' %}
    {% set table_name = 'prodctl_F0005' %}
{% else %}
    {% set table_name = table %}
{% endif %}

{% set n_days = range(0, num_days) %}

{% for n_day in n_days %}
SELECT
    '{{ table }}' AS TABELA,
    COUNT(*) AS QTD_TARGET,
    '{{ current_julian_date - n_day }}' AS JULIAN_AUDIT_DATE
FROM
    {% if env == 'prd' %}
        class_prd.jde_class_raw_prd.{{ table_name }}
    {% else %}
        class_uat.jde_class_raw_uat.{{ table_name }}
    {% endif %}
WHERE
    1 = 1
    {% if table_name not in ('F41112', 'F5549401', 'F5570010', 'F5570011', 'F554349F', 'F5543081', 'F5576523', 'F554201B', 'F5576201') %}
    AND (COALESCE(OP_TYPE, '-') NOT IN ('0', '3') OR (OP_TYPE = '0' AND OP_TIME = 0))
    {% endif %}
    AND {{ audit_column }} = {{ current_julian_date - n_day }}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
