SELECT
    '{{ table }}' AS TABELA,
    COUNT(*) AS QTD_SOURCE
FROM {{ schema }}.{{ table }}
WHERE
    1 = 1
    AND CTMS_TIMESTAMP >= SYSDATE - 7
