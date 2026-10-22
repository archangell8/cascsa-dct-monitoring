import ast
import pymsteams
import logging
import pandas as pd

from typing import Any
from datetime import datetime
from pathlib import Path

from . import controls, databases, mail

PATH_HOME = Path(__file__).parent.parent

def compare_dataframes(
    source_dfs: list[pd.DataFrame],
    snowflake_dfs: list[pd.DataFrame],
    cdp_dfs: list[pd.DataFrame],
    source_name: str
) -> pd.DataFrame:
    """Compara DataFrames de origem e Snowflake

    Args:
        source_dfs: Lista de DataFrames da fonte original
        snowflake_dfs: Lista de DataFrames do Snowflake
        source_name: Nome da fonte de dados ('jde', 'pscfl', etc.)

    Returns:
        DataFrame consolidado com as comparações
    """
    # Concatena todos os DataFrames
    df_source = pd.concat(source_dfs, ignore_index=True)
    df_snowflake = pd.concat(snowflake_dfs, ignore_index=True)

    # Realiza o merge conforme o tipo de fonte
    if source_name == 'jde':
        df_merged = pd.merge(
            df_source,
            df_snowflake,
            on=['TABELA', 'JULIAN_AUDIT_DATE'],
            how='outer'
        )

        df_merged['AUDIT_DATE'] = (
            df_merged['JULIAN_AUDIT_DATE'].apply(
                controls.transform_julian_to_gregorian
            )
        )

    else:
        df_merged = pd.merge(
            df_source,
            df_snowflake,
            on=['TABELA'],
            how='outer'
        )

    # Calcula diferenças e adiciona timestamp
    df_merged['TIMESTAMP_EXECUTION'] = (
        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    )

    df_merged['DIFF_SF_SOURCE'] = (
        df_merged[f'QTD_SOURCE'] - df_merged['QTD_TARGET']
    )

    if source_name == 'jde_full' and cdp_dfs:

        df_cdp = pd.concat(cdp_dfs, ignore_index=True)

        df_merged = pd.merge(
            df_merged,
            df_cdp,
            on=['TABELA'],
            how='outer'
        )

        df_merged['DIFF_SF_CDP'] = (
            df_merged[f'QTD_CDP'] - df_merged['QTD_TARGET']
        )

        df_merged = df_merged[['TABELA', 'QTD_SOURCE', 'QTD_TARGET', 'QTD_CDP', 'DIFF_SF_SOURCE', 'DIFF_SF_CDP', 'AUDIT_DATE', 'TIMESTAMP_EXECUTION']]


    return df_merged

def run_ingestion_monitoring(
    env: str,
    conn_snowflake: Any,
    config_db: dict,
    config_tables: str,
    db: str,
    logger: logging.Logger
) -> pd.DataFrame:
    """Run the ingestion monitoring and get the differences in a dataframe.

    Args:
        env (str): Enviroment to run the monitoring.
        conn_snowflake: Connection with the snowflake.
        config_db (dict): Dictionary with the databases configurations.
        config_tables (dict): Dictonary with the tables configurations.
        db (str): Database source to read.
        logger: Object logger

    Returns:
        DataFrame com os resultados
    """
    # Create source connection
    conn_source = databases.get_connection(
        db_type='transactional',
        config=config_db[db]
    )

    conn_cdp = None
    # schema_cdp = None
    schema_cdp = 'stg_internal_class'

    source_dfs = []
    sf_dfs = []
    cdp_dfs = []

    # To improve
    source_key = db.replace('_full', '') if 'full' in db else db

    # Get the dictionary of tables
    dict_tables = ast.literal_eval(config_tables['tables'][source_key])

    for schema, tables in dict_tables.items():
        for table, details in tables.items():
            logger.info(f"\tReading table: {table}")

            # Get default paramns
            params = {
                'table': table,
                'env': env,
                'current_julian_date': controls.get_current_julian_date(),
                'num_days': 3,
                'schema': schema,
                'schema_cdp': schema_cdp,
                **details
            }

            # Get queries
            source_query = controls.read_query(
                './assets/sql/check_delay',
                f'count_source_{db}_tables.sql',
                params
            )

            sf_query = controls.read_query(
                './assets/sql/check_delay',
                f'count_snowflake_{db}_tables.sql',
                params
            )

            # Run source queries
            source_dfs.append(
                databases.execute_query(conn_source, source_query)
            )

            # Run snowflake queries
            sf_dfs.append(
                databases.execute_query(conn_snowflake, sf_query)
            )

            if db == 'jde_full' and env == 'dev':
                #only for drona for now
                conn_cdp = databases.get_connection(
                    db_type='transactional',
                    config=config_db['cdp']
                )

                # get cdp query
                cdp_query = controls.read_query(
                    './assets/sql/check_delay',
                    f'count_cdp_{db}_tables.sql',
                    params
                )

                # Run cdp queries
                cdp_dfs.append(
                    databases.execute_query(conn_cdp, cdp_query)
                )

                conn_cdp.close()

    # Compara resultados
    df = compare_dataframes(
        source_dfs,
        sf_dfs,
        cdp_dfs,
        db
    )

    return df

def get_database_differences(
    args: dict,
    conn_snowflake: Any,
    db_snowflake: str,
    sc_snowflake: str,
    config: dict,
    tables: dict,
    logger: logging.Logger
) -> pd.DataFrame:
    logger.info(f'## {args.source.upper()} source: ')
    table_name = (
        f'{db_snowflake}.{sc_snowflake}.tab_monitoring_count_diff_{args.source}'
    )

    logger.info(f'Get differences between {args.source} and snowflake...')
    df = run_ingestion_monitoring(
        env=args.env,
        conn_snowflake=conn_snowflake,
        config_db=config,
        config_tables=tables,
        db=args.source,
        logger=logger
    )

    logger.info('Loading results on snowflake...')
    databases.load_data_snowflake(
        df=df,
        table_name=table_name,
        conn=conn_snowflake,
    )

    logger.info(f'Sample results for {args.source.upper()}:')
    logger.info(df.head(10))

    return df

def send_teams_report(
    df: pd.DataFrame,
    args: dict,
    config: dict,
    logger: logging.Logger
) -> pd.DataFrame:
    """Sends a data ingestion report to a Microsoft Teams channel using
        a webhook, and return a dataframe with status.

    Args:
        df (Pandas DataFrame): DataFrame containing data.
        args (dict): Dictionary with args informations.
        config (dict): Dictionary with config informations.
        logger (object): Logger object.

    Return:
        Pandas DataFrame: DataFrame with the status ingestion
    """
    # Create a Teams connector card
    teams_message = pymsteams.connectorcard(config['teams'][args.env])

    df = df.rename(
        columns={'TABELA': 'TABLE'}
    )[['TABLE', 'AUDIT_DATE', 'QTD_SOURCE', 'QTD_TARGET', 'DIFF']]

    # Get the rate
    df['DIFF_PERCENT'] = df.apply(
        lambda row: (
            0
            if row['QTD_TARGET'] > row['QTD_SOURCE']
            else abs(row['QTD_TARGET'] - row['QTD_SOURCE']) / row['QTD_SOURCE']
            if row['QTD_SOURCE'] != 0
            else 0
        ),
        axis=1
    )

    #df['DIFF_PERCENT'] = df.apply(
    #    lambda row:
    #        abs(row['QTD_TARGET'] - row['QTD_SOURCE']) / row['QTD_SOURCE']
    #        if row['QTD_SOURCE'] != 0 else 0,
    #    axis=1
    #)


    # Definir o status com base na diferença percentual

    #F5576201 is batch ingestion table, must have diferent percentual from streaming
    if df['TABLE'].items() == 'F5576201':
        df['STATUS'] = (
        df['DIFF_PERCENT'].apply(
            lambda x: 'updated' if abs(x) <= 1 else 'outdated'
        )
    )

    else:
        df['STATUS'] = (
            df['DIFF_PERCENT'].apply(
                lambda x: 'updated' if abs(x) <= 0.3 else 'outdated'
            )
        )

    # Get unique AUDIT_DATE values
    unique_dates = df['AUDIT_DATE'].unique()

    for audit_date in unique_dates:
        # Filter DataFrame for the current AUDIT_DATE
        df_filtered = df[df['AUDIT_DATE'] == audit_date]

        # Create a Teams connector card
        teams_message = pymsteams.connectorcard(config['teams'][args.env])

        # Generate HTML table with styling
        styled_html = (
            df_filtered
                .to_html(justify= "left", index=False)
                .replace(
                    '<td>updated',
                    '<td style="background-color: MediumSeaGreen">updated'
                )
                .replace(
                    '<td>outdated',
                    '<td style="background-color: Tomato">outdated'
                )
        )

        teams_message.title(
            f"Report {args.source.upper()}".upper()
        )
        teams_message.text(
            f"<h1>Audit date: {audit_date}</h1> \n\n{styled_html}"
        )
        teams_message.send()

    return df

def get_previous_alerts(alert_file) -> pd.DataFrame:
    """
    Load the DataFrame with previously reported from the last alert.

    Parameters:
        alert_file (str or Path):
            File path to the CSV containing task alerts from the previous run.

    Returns:
        pd.DataFrame: DataFrame containing the previous inactive tasks.
        If the file does not exist or is empty, returns an empty DataFrame.
    """
    path = Path(alert_file)
    if path.exists():
        try:
            return pd.read_csv(path)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
    else:
        return pd.DataFrame()

def save_current_alerts(df, alert_file):
    """Save the current DataFrame of the alerts to a CSV file for persistence
    between executions.

    Parameters:
        df (pd.DataFrame): DataFrame of alerts from the current run.
        alert_file (Path or str): File path where the DataFrame should be saved.
    """
    df.to_csv(alert_file, index=False)

def get_persisted_alerts(current_df, previous_df) -> pd.DataFrame:
    """
    Returns only the alerts that are present both in current_df and previous_df,
    based on the key column.

    Parameters:
        current_df (pd.DataFrame): DataFrame of the alerts from the current run.
        previous_df (pd.DataFrame): DataFrame of the alerts from the previous run.

    Returns:
        pd.DataFrame: DataFrame containing only the persisted alerts.
    """
    # If one of the dataframes is empty, an empty dataframe is returned.
    # The only case of an alert is if there is data in both dataframes,
    #   as this would be an alert that existed in the last execution and
    #   in the current execution.
    if current_df.empty or previous_df.empty:
        return pd.DataFrame()
    # Merge only on TABLE, keep columns from current_df (avoid duplicates)
    persisted = current_df[current_df['TABLE'].isin(previous_df['TABLE'])]
    return persisted

def send_email_alert(
    df: pd.DataFrame,
    args: dict,
    config: dict,
    logger: logging.Logger
) -> bool:
    # Alert file
    alert_file = f"{PATH_HOME}/output/last_alert_delay_{args.source}_{args.env}.csv"

    # Get just outdated rows
    df_outdated = df[df['STATUS'] == 'outdated']

    if df_outdated.empty:
        logger.info('All tables are up to date, nothing to send in the e-mail.')
        save_current_alerts(df_outdated, alert_file)
        return True

    # Get last alerts and compare with the new outdated rows
    df_previous = get_previous_alerts(alert_file)
    df_persisted = get_persisted_alerts(df_outdated, df_previous)

    if df_persisted.empty:
        logger.info(
            'There are outdated tables, but the next run will be awaited to '
            'check if the problem persists.'
        )
        save_current_alerts(df_outdated, alert_file)
        return True

    try:
        logger.info('Sending email alert to recipients...')

        # Setup output directory
        base_dir = Path(__file__).parent.parent.resolve()
        output_dir = base_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        report_path = output_dir / f"report_delay_{args.source}.csv"

        # Save report
        df.to_csv(report_path, index=False, encoding='utf-8')

        # Select just necessary columns
        df_persisted = df_persisted.rename(
            columns={'AUDIT_DATE': 'DATE', 'QTD_TARGET': 'QTD_SNOWFLAKE'}
        )[['TABLE', 'DATE', 'QTD_SOURCE', 'QTD_SNOWFLAKE', 'STATUS']]

        # Change status column colos
        df_persisted['STATUS'] = df_persisted['STATUS'].replace(
            'outdated', '<b style="color:red">OUTDATED</b>'
        )

        # Build HTML table with the necessary info
        html_table = df_persisted.to_html(
            index=False, escape=False, border=1
        )

        # Read the HTML template
        with open(
            f"{base_dir}/assets/html/email_body_alert_delay.html",
            encoding="utf-8"
        ) as f:
            html_template = f.read()

        # PReplace fields
        body = html_template.format(
            source_db=args.source.upper(),
            environment=args.env.upper(),
            html_table=html_table
        )

        subject = (
            f"[{args.env.upper()}] DCT Ingestion Delay Alert "
            f"- {args.source.upper()} Source"
        )

        email_config = {
            "email_server": config['email']['server'],
            "sender": config['email']['sender'],
            "recipients": config['email']['recipients'],
            "subject": subject,
            "body": body,
            "maintype": "html",
            "subtype": "html"
        }

        mail.send_email(email_config, str(report_path))

        save_current_alerts(df_outdated, alert_file)

    finally:
        try:
            if report_path.exists():
                report_path.unlink()
        except Exception as e:
            logger.warning(
                f"Could not remove temporary report file {report_path}: {e}"
            )

    return True
