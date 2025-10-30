# %%
import pandas as pd
from sqlalchemy import create_engine, text
from typing import List, Optional



# %%
import pandas as pd
from sqlalchemy import create_engine, text
from typing import List, Optional

def df_to_mariadb_idempotent(
    df: pd.DataFrame,
    host: str = "localhost",
    port: int = 3306,
    user: str = "root",
    password: str = "",
    database: str = "my_database",
    table_name: str = "my_table",
    include_columns: Optional[List[str]] = None,
    unique_key_cols: Optional[List[str]] = None,
):
    """
    Insert a (subset of a) DataFrame into MariaDB in an idempotent way:
    running this twice with the same data will NOT create duplicates,
    as long as `unique_key_cols` really identifies a row.

    Steps:
    - create database if not exists
    - create table if not exists (with types inferred from first row)
    - create UNIQUE index on `unique_key_cols`
    - insert rows using INSERT ... ON DUPLICATE KEY UPDATE
    """
    # 1. slice columns
    if include_columns is not None:
        missing = [c for c in include_columns if c not in df.columns]
        if missing:
            raise ValueError(f"Columns not in DataFrame: {missing}")
        df = df[include_columns].copy()

    if unique_key_cols is None:
        raise ValueError("You must provide unique_key_cols to avoid duplicates.")

    for col in unique_key_cols:
        if col not in df.columns:
            raise ValueError(f"unique_key_cols '{col}' not found in DataFrame columns")

    # 2. connect to server (no db)
    root_engine = create_engine(
        f"mariadb+mariadbconnector://{user}:{password}@{host}:{port}/mysql"
    )

    # 3. create database if missing
    with root_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{database}`"))
        conn.commit()

    # 4. connect to target db
    engine = create_engine(
        f"mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{database}"
    )

    # 5. create table if not exists
    # we'll infer a simple table: TEXT for objects, DOUBLE for numerics, DATETIME for datetime
    # (good enough for many cases; can be customized)
    def pandas_dtype_to_sql(dtype) -> str:
        if pd.api.types.is_integer_dtype(dtype):
            return "BIGINT"
        if pd.api.types.is_float_dtype(dtype):
            return "DOUBLE"
        if pd.api.types.is_bool_dtype(dtype):
            return "TINYINT(1)"
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "DATETIME"
        # fallback
        return "TEXT"

    cols_sql_parts = []
    for col in df.columns:
        col_sql_type = pandas_dtype_to_sql(df[col].dtype)
        cols_sql_parts.append(f"`{col}` {col_sql_type}")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {', '.join(cols_sql_parts)}
    ) ENGINE=InnoDB;
    """

    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()

        # 6. create UNIQUE index on the key columns (if not already created)
        # name it deterministically
        index_name = f"uq_{table_name}_" + "_".join(unique_key_cols)
        # Try to create; if it exists, MariaDB will complain, so we guard with IF NOT EXISTS
        # MariaDB supports IF NOT EXISTS for indexes from 10.5+; if yours doesn't, we can query information_schema
        unique_idx_sql = f"""
        CREATE UNIQUE INDEX IF NOT EXISTS `{index_name}`
        ON `{table_name}` ({', '.join(f'`{c}`' for c in unique_key_cols)});
        """
        try:
            conn.execute(text(unique_idx_sql))
            conn.commit()
        except Exception:
            # if IF NOT EXISTS isn't supported, you can skip, or check info_schema first
            pass

        # 7. insert rows with ON DUPLICATE KEY UPDATE
        cols = list(df.columns)
        placeholders = ", ".join([f":{c}" for c in cols])
        col_list_sql = ", ".join(f"`{c}`" for c in cols)

        # if duplicate: we can either NO-OP or update other cols.
        # Here we'll update all non-key columns to the incoming value.
        update_parts = []
        for c in cols:
            if c not in unique_key_cols:
                update_parts.append(f"`{c}` = VALUES(`{c}`)")
        if update_parts:
            update_sql = "ON DUPLICATE KEY UPDATE " + ", ".join(update_parts)
        else:
            # if the whole row is the key, then do nothing on duplicate
            update_sql = "ON DUPLICATE KEY UPDATE " + ", ".join(
                f"`{k}`=`{k}`" for k in unique_key_cols
            )

        insert_sql = f"""
        INSERT INTO `{table_name}` ({col_list_sql})
        VALUES ({placeholders})
        {update_sql};
        """

        # execute in batches
        records = df.to_dict(orient="records")
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            conn.execute(text(insert_sql), batch)
        conn.commit()

    print(f"✅ Upserted {len(df)} row(s) into {database}.{table_name}")


if __name__ == "__main__":
    # example
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
        "age": [25, 30, 22],
        "city": ["Paris", "Berlin", "Rome"],
    }
    df = pd.DataFrame(data)

    # Suppose "id" uniquely identifies a row
    df_to_mariadb_idempotent(
        df,
        host="localhost",
        port=3306,
        user="root",
        password="mypassword",
        database="people_db",
        table_name="people",
        include_columns=["id", "name", "age"],  # only these 3 go to DB
        unique_key_cols=["id"],                  # 'id' must be unique
    )


# %%
f1 = "../data/sihsus/RDCE0801.parquet"
df1 = pd.read_parquet(f1)
cols1 = set(df1.columns)


# %%
f2 = "../data/sihsus/RDCE2508.parquet"
df2 = pd.read_parquet(f2)
cols2 = set(df2.columns)
