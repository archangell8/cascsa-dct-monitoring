SELECT
    ENTITY,
    ENTITY_KEY,
    FIELD_NOT_OK,
    VALUE_NOT_OK,
    COMMENTS
FROM
    {% if env == 'prd' %}
        casc_digital_control_tower_prd.digital_control_tower_trns.{{ table }}
    {% else %}
        casc_digital_control_tower_qas.digital_control_tower_trns.{{ table }}
    {% endif %}
WHERE
    1 = 1
