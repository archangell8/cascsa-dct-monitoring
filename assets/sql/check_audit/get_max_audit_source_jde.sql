SELECT
    '{{ table }}' TABELA,
    SAD.FX_PARA_GREGORIANA(MAX({{ audit_column }}), 'YYYY-MM-DD') MAX_AUDIT_DATE_SOURCE
FROM {{ schema }}.{{ table }}
WHERE
    1 = 1