import json
import os
from typing import Any

import jaydebeapi
import pandas as pd
import requests
import snowflake.connector

OAUTH_ENDPOINT = (
    "https://login.microsoftonline.com/"
    "57368c21-b8cf-42cf-bd0b-43ecd4bc62ae/oauth2/v2.0/token"
)


def _get_snowflake_token(db_type: str) -> str:
    """Obtém token OAuth para autenticação no Snowflake"""
    env_suffix = "dev" if "dev" in db_type else "prd"
    env_suffix = env_suffix.upper()

    oauth_data = {
        "grant_type": "client_credentials",
        "client_id": os.getenv(f'SNOWFLAKE_{env_suffix}_CLIENT_ID'),
        "client_secret": os.getenv(f'SNOWFLAKE_{env_suffix}_SECRET'),
        "scope": os.getenv(f'SNOWFLAKE_{env_suffix}_SCOPE'),
    }
    
    try:
        response = requests.post(OAUTH_ENDPOINT, data=oauth_data, timeout=30)
        response.raise_for_status()
        return json.loads(response.text)["access_token"]
        
    except Exception as e:
        raise Exception(f"Falha ao obter token OAuth: {str(e)}") from e


def get_connection(db_type: str, config: dict[str, Any]):
    """Gerencia conexões com Oracle, SQL Server e Snowflake automaticamente
    
    Args:
        db_type: Tipo de banco.
        config: Dicionário com configurações de conexão.
        
    Yields:
        Conexão ativa com o banco de dados
    """
    conn = None
    try:
        if "snowflake" in db_type:
            token = _get_snowflake_token(db_type)
            conn = snowflake.connector.connect(**config, token=token)
        
        elif db_type in ("transactional"):
            conn = jaydebeapi.connect(**config)
        
        else:
            raise Exception(f"Tipo de banco não suportado: {db_type}")
        
        return conn
    except Exception as e:
        raise Exception(f"Erro na conexão com {db_type}: {str(e)}") from e


def execute_query(conn, query: str) -> pd.DataFrame:
    """Executa uma query SQL e retorna os resultados como DataFrame
    
    Args:
        conn: Conexão ativa com o banco
        query: Consulta SQL a ser executada
        
    Returns:
        DataFrame com os resultados
    """
    try:
        df = pd.read_sql(query, conn)

        df.columns = [str(c).upper() for c in df.columns]

        return df 
    
    except Exception as e:
        raise Exception(
            f"Erro ao executar query: {str(e)}\nQuery: {query[:200]}..."
        ) from e


def load_data_snowflake(
    df: pd.DataFrame,
    table_name: str,
    conn
):
    """Load a dataframe in snowflake.
    
    Args:
        df: Dataframe with the data.
        table_name (str): Table name (format: database.schema.table).
        conn: Snowflake connection.
    """
    # Drop table
    drop_statement = f"DROP TABLE IF EXISTS {table_name}"
    
    # Create table
    create_statement = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    create_statement += ',\n'.join([f"{col} STRING" for col in df.columns])
    create_statement += "\n);"

    # Initializate list to save INSERT values
    insert_values = []
    
    # Iterate each row of dataframe
    for _, row in df.iterrows():
        # Create a list to each row
        values = []
        for value in row:
            if pd.isna(value):
                values.append('NULL')
            elif isinstance(value, str):
                values.append(f"'{value}'")
            else:
                values.append(f"'{value}'")
             
        # Append values to create INSERT
        insert_values.append(f"({', '.join(values)})")
    
    # Create INSERT statement
    insert_statement = (
        f"INSERT INTO {table_name} "
        f"VALUES {', '.join(insert_values)};"
    )
    
    # Run commands
    conn.cursor().execute(drop_statement)
    conn.cursor().execute(create_statement)
    conn.cursor().execute(insert_statement)
