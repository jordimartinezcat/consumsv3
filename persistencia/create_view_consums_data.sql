-- Vista para mostrar datos de consumo con correcciones aplicadas
-- Si existe una corrección para la misma fecha y tag, se muestra la corrección
-- en lugar del valor original

CREATE OR REPLACE VIEW ga_datalake.v_ite_consums_data AS
SELECT 
    d.data,
    d.idtag,
    COALESCE(r.valor, d.valor) AS valor,
    d.data_insercio,
    CASE 
        WHEN r.id IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS te_correccio,
    r.tipus AS tipus_correccio,
    r.descrip AS descrip_correccio
FROM 
    ga_datalake.ite_consums_data d
LEFT JOIN LATERAL (
    SELECT id, valor, tipus, descrip
    FROM ga_datalake.ite_consums_datarect
    WHERE data = d.data 
      AND idtag = d.idtag
    ORDER BY data_insercio DESC, id DESC
    LIMIT 1
) r ON TRUE
ORDER BY 
    d.data, d.idtag;

COMMENT ON VIEW ga_datalake.v_ite_consums_data IS 
'Vista consolidada de datos de consumo. Muestra el valor corregido de ite_consums_datarect si existe, 
de lo contrario muestra el valor original de ite_consums_data. 
Incluye flags para identificar registros con corrección.';
