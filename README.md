# consumsv3

## Señales extraídas (consulta)

El script `adquisicion/extraer_senales_ftr.py` implementa la lógica de extracción de señales para procesar consumos:

- Busca directamente señales que terminan en `TOT_L` y `TOT_H`.
- Excluye señales que empiecen por `ET` y cualquiera que contenga `_LS_` o `_P_`.
- Escapa el carácter `_` en patrones `LIKE` para forzar coincidencia literal en PostgreSQL.
- Filtra las señales `*TOT` generales para conservar sólo aquellas cuyo prefijo (primeros 5 caracteres) no esté presente entre las señales `TOT_L`/`TOT_H` encontradas.
- Escribe la lista final en `adquisicion/senales_para_descarga.txt`.

Ejecuta:

```pwsh
python .\adquisicion\extraer_senales_ftr.py
```

El script generará `adquisicion/senales_para_descarga.txt` con una señal por línea.
