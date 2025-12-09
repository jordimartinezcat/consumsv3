-- Vista para mostrar consumos diarios (24h) agrupados desde datos horarios
-- Suma los consumos horarios por día y tag
-- Mantiene información de correcciones aplicadas

CREATE OR REPLACE VIEW ga_datalake.v_ite_consums_24h AS
SELECT 
    (DATE_TRUNC('day', data AT TIME ZONE 'Europe/Madrid') AT TIME ZONE 'Europe/Madrid')::timestamptz AS data,
    idtag,
    SUM(valor) AS valor,
    MAX(data_insercio) AS data_insercio,
    BOOL_OR(te_correccio) AS te_correccio,
    MAX(tipus_correccio) AS tipus_correccio,
    CASE 
        -- Si no hay correcciones, NULL
        WHEN NOT BOOL_OR(te_correccio) THEN NULL
        -- Si hay correcciones pero diferentes mensajes, mensaje genérico
        WHEN COUNT(DISTINCT descrip_correccio) FILTER (WHERE te_correccio) > 1 THEN 'Varies incidències corregides'
        -- Si todas las correcciones tienen el mismo mensaje, mostrar ese mensaje
        ELSE MAX(descrip_correccio) FILTER (WHERE te_correccio)
    END AS descrip_correccio
FROM 
    ga_datalake.v_ite_consums_data
GROUP BY 
    DATE_TRUNC('day', data AT TIME ZONE 'Europe/Madrid'), idtag
ORDER BY 
    data, idtag;

COMMENT ON VIEW ga_datalake.v_ite_consums_24h IS 
'Vista de consumos diarios (24h) agrupados desde datos horarios.
Suma los valores horarios por día y tag.
Si hay correcciones horarias con diferentes mensajes: "Varies incidències corregides".
Si todas las correcciones tienen el mismo mensaje: muestra ese mensaje.
Si no hay correcciones: descrip_correccio es NULL.';
