SELECT
    MODEL_NAME AS ENTITY,
    CHAVE AS ENTITY_KEY,
    QTD_RECORDS AS QUANTITY_OF_DUPLICATES
FROM (
    {% for table in tables %}
      SELECT * FROM 
      {% if env == 'prd' %}
          casc_digital_control_tower_prd.digital_control_tower_trns.{{ table }}
      {% else %}
          casc_digital_control_tower_qas.digital_control_tower_trns.{{ table }}
      {% endif %}
      {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)
