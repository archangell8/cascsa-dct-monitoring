SELECT
    '{{ table }}' TABELA,
    MAX({{ audit_column }}) MAX_AUDIT_DATE_SOURCE
FROM {{ schema }}.{{ table }}
WHERE
    1 = 1