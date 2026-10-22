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
    snowflake_prd: list[pd.DataFrame],
    snowflake_dev: list[pd.DataFrame],
    source_name: str
) -> pd.DataFrame:
    """Compara DataFrames de Snowflake prod e Snowflake dev

    Args:
        snowflake_prd: Lista de DataFrames do Snowflake prod
        snowflake_dev: Lista de DataFrames do Snowflake dev
        source_name: Nome da fonte de dados (only 'jde' available for now)

    Returns:
        DataFrame consolidado com as comparações
    """
    # Concatena todos os DataFrames
    df_snowflake_prd = pd.concat(snowflake_prd, ignore_index=True)
    df_snowflake_dev = pd.concat(snowflake_dev, ignore_index=True)

    results = []

    # Realiza o merge conforme o tipo de fonte
    if source_name == 'jde':
        df_merged = pd.merge(
            df_snowflake_prd,
            df_snowflake_dev,
            on=['TABELA'],
            how='outer'
        )

        df_merged = df_merged[
            ['TABELA', 'MAX_KAFKA_OFFSET_DEV', 'MAX_KAFKA_CREATETIME_DEV', 'MAX_KAFKA_OFFSET_PRD', 'MAX_KAFKA_CREATETIME_PRD']
        ]

        dt_dev = pd.to_datetime(
            df_merged['MAX_KAFKA_CREATETIME_DEV'], errors='coerce'
        )
        dt_prd = pd.to_datetime(
            df_merged['MAX_KAFKA_CREATETIME_PRD'], errors='coerce'
        )

        # Get the dataframe difference
        diff_days = (dt_prd - dt_dev).abs() / pd.Timedelta(days=1)
        df_merged['DIFF_DAYS'] = diff_days

        results.append(df_merged)

    # else:
    #     df_merged = pd.merge(
    #         df_source,
    #         df_snowflake,
    #         on=['TABELA'],
    #         how='outer'
    #     )

    # # Calcula diferenças e adiciona timestamp
    # df_merged['TIMESTAMP_EXECUTION'] = (
    #     datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # )

    # df_merged['DIFF'] = (
    #     df_merged[f'QTD_SOURCE'] - df_merged['QTD_TARGET']
    # )

    df_final = pd.concat(results, ignore_index=True)

    # print(df_final)

    return df_final

def run_kafka_monitoring(
    conn_snowflake_prd: Any,
    conn_snowflake_dev: Any,
    config_tables: str,
    db: str,
    logger: logging.Logger
) -> pd.DataFrame:
    """Run the kafka monitoring and get the differences in a dataframe.

    Args:
        conn_snowflake_prd: Connection with the Snowflake production environment.
        conn_snowflake_dev: Connection with the Snowflake development environment.
        config_db_prd (dict): Dictionary with the production database configurations.
        config_db_dev (dict): Dictionary with the development database configurations.
        config_tables (dict): Dictionary with the tables configurations.
        db (str): Database source to read.
        logger: Object logger

    Returns:
        DataFrame com os resultados
    """
    dev_offset = []

    prd_offset = []

    # To improve
    source_key = db.replace('_full', '') if 'full' in db else db

    # Get the dictionary of tables
    dict_tables = ast.literal_eval(config_tables['tables'][source_key])

    for schema, tables in dict_tables.items():
        for table, details in tables.items():
            logger.info(f"\tReading table: {table}")

            # Get default paramns
            params_dev = {
                'table': table,
                'env': 'dev'
            }

            params_prd = {
                'table': table,
                'env': 'prd'
            }

            # Get queries
            dev_query = controls.read_query(
                './assets/sql/check_kafka_offset',
                f'get_max_offset_snowflake_{db}.sql',
                params_dev
            )

            prd_query = controls.read_query(
                './assets/sql/check_kafka_offset',
                f'get_max_offset_snowflake_{db}.sql',
                params_prd
            )

            # Run source queries
            dev_offset.append(
                databases.execute_query(conn_snowflake_dev, dev_query)
            )

            # Run snowflake queries
            prd_offset.append(
                databases.execute_query(conn_snowflake_prd, prd_query)
            )

    # Compara resultados
    df = compare_dataframes(
        prd_offset,
        dev_offset,
        db
    )

    return df

def get_database_differences(
    args: dict,
    conn_snowflake_dev: Any,
    conn_snowflake_prd: Any,
    tables: dict,
    logger: logging.Logger
) -> pd.DataFrame:
    logger.info(f'## {args.source.upper()} source: ')
    # table_name = (
    #     f'{db_snowflake}.{sc_snowflake}.tab_monitoring_kafka_{args.source}'
    # )

    logger.info(f'Get kafka differences between snowflake dev and snowflake prd...')
    df = run_kafka_monitoring(
        conn_snowflake_prd=conn_snowflake_prd,
        conn_snowflake_dev=conn_snowflake_dev,
        config_tables=tables,
        db=args.source,
        logger=logger
    )

    # logger.info('Loading results on snowflake...')
    # databases.load_data_snowflake(
    #     df=df,
    #     table_name=table_name,
    #     conn=conn_snowflake,
    # )

    logger.info(f'Sample results for {args.source.upper()}:')
    logger.info(df.head(10))

    return df

def get_previous_alerts(alert_file) -> pd.DataFrame:
    """
    Load the DataFrame with previously reported inactive tasks from the last alert.

    Parameters:
        alert_file (str or Path): File path to the CSV containing task alerts from the previous run.

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

    # Merge only on TABELA, keep columns from current_df (avoid duplicates)
    persisted = (
        current_df[current_df['TABELA'].isin(previous_df['TABELA'])]
    )

    return persisted

def send_email_alert(
    df: pd.DataFrame,
    args: dict,
    config: dict,
    logger: logging.Logger
) -> True:
    # Change the days to generate alert based on the source
    if args.source == 'jde':
        time_alert = 2
    else:
        time_alert = 1

    # File to save the current alerts
    alert_file = f"{PATH_HOME}/output/last_alert_kafka_{args.source}_{args.env}.csv"

    # Filter rows to alert
    df_alert = df[pd.to_numeric(df['DIFF_DAYS']) >= time_alert]

    # If we have any data in the dataframe
    if df_alert.empty:
        logger.info(
            f'All tables are updated in the last {time_alert} day(s). '
            'Nothing to do.'
        )
        save_current_alerts(df_alert, alert_file)
        return True

    # Get last alerts and compare with the new outdated rows
    df_previous = get_previous_alerts(alert_file)
    df_persisted = get_persisted_alerts(df_alert, df_previous)

    if df_persisted.empty:
        logger.info(
            f'There are outdated tables in the last {time_alert} day(s) but '
            'the next run will be awaited to check if the problem persists.'
        )
        save_current_alerts(df_alert, alert_file)
        return True

    try:
        logger.info('Sending email alert to recipients...')

        # Setup output directory
        base_dir = Path(__file__).parent.parent.resolve()
        output_dir = base_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        report_path = output_dir / f"report_kafka_{args.source}.csv"

        # Save report
        df_persisted.to_csv(report_path, index=False, encoding='utf-8')


        # Build HTML table with the necessary info
        df_persisted_html = df_persisted[
            ['TABELA', 'MAX_KAFKA_CREATETIME_DEV', 'MAX_KAFKA_CREATETIME_PRD', 'DIFF_DAYS']
        ]
        html_table = df_persisted_html.to_html(index=False, escape=False, border=1)

        # Read the HTML template
        with open(
            f"{base_dir}/assets/html/email_body_alert_kafka.html",
            encoding="utf-8"
        ) as f:
            html_template = f.read()

        # PReplace fields
        body = html_template.format(
            source_db=args.source.upper(),
            environment=args.env.upper(),
            time_alert=time_alert,
            html_table=html_table
        )

        subject = (
            f"[{args.env.upper()}] DCT Kafka Offset Alert "
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

        save_current_alerts(df_alert, alert_file)

    finally:
        try:
            if report_path.exists():
                report_path.unlink()
        except Exception as e:
            logger.warning(
                f"Could not remove temporary report file {report_path}: {e}"
            )

    return True