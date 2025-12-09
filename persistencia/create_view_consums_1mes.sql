-- Vista para mostrar consumos mensuales agrupados desde datos horarios
-- Suma los consumos horarios por mes y tag
-- Indica si el mes contiene alguna corrección aplicada

CREATE OR REPLACE VIEW ga_datalake.v_ite_consums_1mes AS
SELECT 
    (DATE_TRUNC('month', data AT TIME ZONE 'Europe/Madrid') AT TIME ZONE 'Europe/Madrid')::timestamptz AS data,
    idtag,
    SUM(valor) AS valor,
    MAX(data_insercio) AS data_insercio,
    BOOL_OR(te_correccio) AS te_correccio,
    MAX(tipus_correccio) AS tipus_correccio
FROM 
    ga_datalake.v_ite_consums_data
GROUP BY 
    DATE_TRUNC('month', data AT TIME ZONE 'Europe/Madrid'), idtag
ORDER BY 
    data, idtag;

COMMENT ON VIEW ga_datalake.v_ite_consums_1mes IS 
'Vista de consumos mensuales agrupados desde datos horarios.
Suma los valores horarios por mes y tag.
El campo te_correccio indica si el mes contiene alguna hora corregida (TRUE/FALSE).';
