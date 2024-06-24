'''


'''
import pandas as pd
from sqlalchemy import text

def perform_query(query_str, engine, batchsize=1000):
    '''
        ...

        Args:
        -----
            query_str:
                String. SQL query.
            engine:
                sqlalchemy.engine.base.Engine. engine used for database connection.

        Return:
        -------
            res_df:
                pandas.DataFrame. Dataframe corresponding to the fetched data from the SQL query.
    '''
    schema_data = {
        'rows': [],
        'columns': [],
    }

    with engine.connect() as conn:
        qres = conn.execute(text(query_str))
        schema_data['columns'] = list(qres.keys())

        while True:
            rows = qres.fetchmany(batchsize)
            if not rows:
                break
            schema_data["rows"] += [ row for row in rows ]
    
    res_df = pd.DataFrame(schema_data['rows'], columns=schema_data['columns'])
    return res_df