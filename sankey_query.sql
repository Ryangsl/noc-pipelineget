-- =============================================================
--  NOC Pipeline — Dados para Sankey (tickets únicos por fluxo)
-- =============================================================
-- Fluxo: systemOrigin → typeEvent → technology
-- Cada ticket_id é contado UMA vez por caminho.
-- =============================================================

SELECT
    system_origin   AS source,
    type_event      AS event,
    technology      AS target,
    COUNT(DISTINCT ticket_id) AS total
FROM history_io
WHERE system_origin IS NOT NULL
  AND type_event    IS NOT NULL
  AND technology    IS NOT NULL
GROUP BY system_origin, type_event, technology
ORDER BY total DESC;
