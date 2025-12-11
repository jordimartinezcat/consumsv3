import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://postgres:P7alxpit@localhost:5432/goaigua_data")

with engine.connect() as conn:
    result = conn.execute(
        text(
            """
        SELECT data, idtag, valor, tipus, descrip, data_insercio 
        FROM ga_datalake.ite_consums_datarect 
        WHERE idtag = 5393 
        AND data BETWEEN '2025-06-21 11:00:00' AND '2025-06-21 16:00:00'
        ORDER BY data
    """
        )
    )
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    print(df.to_string())
